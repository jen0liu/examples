
import java.io._

import org.apache.commons.lang3.StringEscapeUtils
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer


class FilterModel(clusterclient: ConfiguredClusterClient,
                  fullKPI: Seq[(DateTime, DateTime, Double)],
                  metricType: String,
                  preProcessType: String,
                  scalingType: String,
                  holdOutSamples: Int = 0,
                  includeTotalConsumers: Boolean,
                  outputDir: String,
                  fromFile: Boolean,
                  clustered: Double,
                  maxTerms: Int,
                  threshold: Double,
                  bootstrapIterations: Int,
                  percentValidation: Double,
                  resampleCounts: Boolean = true,
                  verbose: Int,
                  output: Option[GoogleChart] = None) {

  val kpiData = fullKPI.slice(0, fullKPI.length - holdOutSamples)
  val kpiValues = kpiData.map { x => x._3 }
  val periods = kpiData.map { x => (x._1, x._2) }

  lazy val consumerCounts = clusterclient.getTimeSeries(periodsAsOffsets, None)
  lazy val sampleCounts: Option[Seq[Double]] = if (scalingType == "vertical" || scalingType == "spritzerSample") Some(clusterclient.getSampleTimeSeries(periodsAsOffsets)) else None
  lazy val CountsMapRaw = if (includeTotalConsumers) clusterclient.getDateEntity(periods).map(p => ("`" + p._1 + "`") -> p._2) ++ Map("TotalConsumer" -> consumerCounts.toIndexedSeq)
                          else clusterclient.getDateEntity(periods).map(p => ("`" + p._1 + "`") -> p._2)

  /* STEP 1 : Reduce terms prior to touching KPI */
  /* Currently: Greedy clustering based on correlation of entity time-series. */
  if (clustered < 1.0) println("*** Reducing Terms ***")
  lazy val CountsMap = if (clustered >= 1.0) CountsMapRaw
                       else entityCluster(clusterclient, CountsMapRaw, periods, clustered, clustered)
  val periodsAsOffsets = datePeriodToOffsets(periods)

  /* Import Term Counts - including Total Consumer counts and/or Tweet Sample counts if required for scaling */
  val scaling = scalingType.toLowerCase match {
    case "consumer" => Some(clusterclient.getTimeSeries(periodsAsOffsets, None))
    case "sample" => Some(clusterclient.getQfSampleTimeSeries(periodsAsOffsets))
    case _ => None
  }
  /* Use Metric Selector to Get Correlation Type & Fit Method given the corrType argument */
  val preProcess = preProcessSelector()
  val model = metricSelector()

  def preProcessSelector(): preProcess[Double] = {
    val preProcessArgList = preProcessType.split(",(?=(^\\))*(?:\\(|$))", -1)
    val preProcessSteps = preProcessArgList.map { arg =>
      val argList = arg.split(",")
      argList.head.toLowerCase match {
        case "none" => new identity
        case "scaling" | "scale" | "rescale" => new rescale(scaling)
        case "smooththenrescale" | "smooththenscale" =>
          argList(1) match {
            case "auto" => new smoothThenRescale(scaling, selectGlobalLag(correlation, new rescale(scaling), 12) + 1)
            //case "auto" => new smoothThenRescale(scaling, selectSmoothing(correlation, 12) + 1)
            case _ => new smoothThenRescale(scaling, argList(1).toInt)
          }

        case "rescalethensmooth" | "scalethensmooth" =>
          argList(1) match {
            case "auto" => new rescaleThenSmooth(scaling, selectGlobalLag(correlation, new rescale(scaling), 12) + 1)
            case _ => new rescaleThenSmooth(scaling, argList(1).toInt)
          }
      }
    }
    preProcessSteps.head
  }

  /**
   * metricType String Encoding the Type of Metric to be used + Parameters needed to define
   * Returns a supervisedModel class that bundles the correlation, fit, etc methods.
   */
  def metricSelector(): TimeSeriesModel[Double, (Double, Double)] = {

    val (metric, parameter) = metricType.split(",") match {
      case Array(mt) => (mt, None)
      case Array(mt, mp) => (mt, Some(mp))
      case _ => throw new Exception("Unable to parse $metricParams into (metrictype, parameter)")
    }

    metric match {
      case "correlation" => new leastSquares
      case "smoothed" =>
        parameter match {
          case Some("auto") => new smoothed(Some(selectGlobalLag(correlation, preProcess, 12) + 1))
          case _ => new smoothed(parameter.map{_.toInt})
        }
    }
  }

  def selectGlobalLag(metric: (Seq[Double], Seq[Double], Option[Seq[Double]]) => Double,
                      preProcess: preProcess[Double],
                      maxLag: Int): Int = {

    println("*** Auto Selecting Global Lag ...")
    val thresh = computeThresholds(metric, preProcess, FilterModel.sidac(threshold, CountsMap.size), 10).head
    val laggedCorr = CountsMap.mapValues {x => for (i <- 0 to maxLag) yield metric(preProcess.process(x).dropRight(i), kpiValues.drop(i), None)}

    val aboveThresh = laggedCorr.toSeq.map{case (term,correlations) => correlations}.transpose.map {
      _.count(_ > thresh)
    }
    aboveThresh.zipWithIndex.foreach{case (v,i) => println(s"Lag $i : Above Thresh $v")}

    output.get.text("Terms correlating above threshold per lag.","")
    output.get.table((0 to maxLag).toSeq.map{_.toString},"Lags", Seq(("Above Threshold", aboveThresh)))

    if (verbose == 5) {
      val xrange = periods.map { x => x._2.toYearMonthDay.toString }
      output.get.heatMap(laggedCorr.toSeq, "terms", (0 to 12).toSeq.map {_.toString})
    }

    val lag = aboveThresh.zipWithIndex.max._2
    println("*   Lag : " + lag)
    println("   ... Select Global Lag Complete ***")
    lag
  }

  def selectSmoothing(metric: (Seq[Double], Seq[Double], Option[Seq[Double]]) => Double,
                      maxWindow: Int): Int = {
    println("*** Auto Selecting Best Smoothing ...")
    val counts = for (w <- 1 to maxWindow) yield {
      val pre = new smoothThenRescale(scaling,w)
      val thresh = computeThresholds(metric, pre, FilterModel.sidac(threshold, CountsMap.size), 10).head
      //val thresh = FilterModel.sidacCorrection(CountsMap, kpiValues, metric, pre, threshold)
      CountsMap.toSeq.map{case (k,x) => metric(pre.process(x), kpiValues, None)}.count{_>thresh}
    }
    counts.zipWithIndex.foreach{case (v,i) => println(s"Window ${i+1} : Above Thresh $v")}
    val lag = counts.zipWithIndex.max._2+1
    println("*   Lag : " + lag)
    println("   ... Select Global Lag Complete ***")
    lag
  }

  def selectTermsByRanking(maxVariables: Int,
                           bootstrapIters: Int): Seq[String] = {

    //val (randomMean, randomStdDev) = normalizeMetric(CountsMap, kpi, metric, preProcess, bootstrapIters, gviz)

    val pValues = FilterModel.sidacHolm(threshold,CountsMap.size,maxVariables)
    val thresholdsPerRank = computeThresholds(model.similarity,preProcess, pValues,25)

    val topFeatureCounts = scala.collection.mutable.Map[String, Double]()

    println("*** Bootstrapped Term Ranking ...")
    val rankingStats = bootStrapIt(bootstrapIters, 0.90, 2) { x =>
      val (train, eval) = trainEvalSplit(kpiValues.length, percentValidation)
      val outputstats = FilterModel.rankTerms(CountsMap, kpiValues, model.similarity, preProcess, thresholdsPerRank.head, Some(train), maxVariables, resampleCounts)
      for ((k, v) <- outputstats(2)) {
        if (topFeatureCounts contains k) topFeatureCounts(k) += 1.0 / bootstrapIters
        else topFeatureCounts(k) = 1.0 / bootstrapIters
      }
      outputstats.slice(0, 2)
    }
    val trackedCorrelations = rankingStats.head
    val trackedRankings = rankingStats(1)

    val orderedFeatures: Seq[(String, Double)] = trackedCorrelations.meanValues.toSeq.sortWith(_._2 > _._2)
    val aboveThresh = orderedFeatures.zip(thresholdsPerRank).filter { case ((entity, correlation),thresh) => correlation > thresh }.map{case ((e,c),t) => e}

    println(s"... Correlations with KPI above Null Hypothesis: ${aboveThresh.length}")

    var topFeatures = aboveThresh.slice(0, maxVariables)
    if (topFeatures.isEmpty) {
      println("... No significant terms! Taking best single entity")
      topFeatures = Seq(orderedFeatures.head._1)
    }

    if (output.isDefined) {
      println("... Output Ranking Stats ***")
      val termsToDisplay = orderedFeatures.slice(0, 5 * maxVariables).map {_._1}
      //val highlightThresh = if (topFeatures.length == maxVariables) orderedFeatures(maxVariables)._2 else thresh
      val highlightThresh = thresholdsPerRank(topFeatures.length-1)

      //gviz.get.table(Seq("Correlation w/ KPI"), "Random Case Performance", Seq(("Mean", Seq(randomMean)), ("Std Deviation", Seq(randomStdDev))),7)

      output.get.table(termsToDisplay, "Terms",
        Seq(("Avg. Counts per Period", termsToDisplay.map { e => CountsMap(e).sum / CountsMap(e).length }),
          ("Mean Correlation", termsToDisplay.map {trackedCorrelations(_).mean}),
          ("Above Random ?", termsToDisplay.zip{thresholdsPerRank}.map {case (e,thresh) => 2*(trackedCorrelations(e).mean - thresh) / (trackedCorrelations(e).mean - trackedCorrelations(e).head()._1) }),
          ("Threshold", thresholdsPerRank),
          ("Corr CI Min", termsToDisplay.map {trackedCorrelations(_).head()._1}),
          ("Corr CI Max", termsToDisplay.map {trackedCorrelations(_).head()._2}),
          ("Mean Ranking", termsToDisplay.map {trackedRankings(_).mean}),
          ("Percent in Top Feature Set", termsToDisplay.map { e => 100.0 * topFeatureCounts.getOrElse(e, 0.0) })),
        height = Some(800),
        highlight = Some((3, highlightThresh)))
    }
    topFeatures
  }

  def computeThresholds(metric: (Seq[Double], Seq[Double], Option[Seq[Double]]) => Double,
                        preProcess: preProcess[Double],
                        pValues: Seq[Double],
                        prec: Int): Seq[Double] = {

    import com.qf.util.conversions.collection._
    import scala.concurrent.ExecutionContext.Implicits.global

    println("*** Computing Thresholds for Correlation ...")
    val threads = 8
    val perThread = (prec * 1 / pValues.head).toInt / threads
    val iters = threads * perThread
    val topN = (pValues.last * iters + 1).toInt
    val terms = CountsMap.keys.toIndexedSeq
    val out: Seq[BoundedPriorityQueue[Double]] =
      (1 to threads).toList.pmap { i =>
        val topValues = BoundedPriorityQueue[Double](topN)
        for (j <- 1 to perThread) {
          val x: Double = metric(preProcess.process(FilterModel.randomTimeSeries(terms, CountsMap, kpiValues.length)), kpiValues, None)
          topValues.enqueue(x)
        }
        topValues
      }
    val mainQueue = out.head
    for (q <- 1 to threads - 1) {
      out(q).toSeq.foreach {
        mainQueue.enqueue(_)
      }
    }
    val samples = mainQueue.toSeq.sortWith(_ > _)
    val thresholds = pValues.map { p => samples((p * iters).toInt - 1) }
    thresholds
  }

  def tuneFilterSize(features: Seq[String],
                     entityPenalty: Boolean = false): Option[String] = {

    output.get.text(
      s"""Filter size automatically selected by incrementally adding terms to filter looking for best R squared value.
         |Outputs scatter plot of precision (R^2 value) vs recall (average user counts per time period) for the various
         |filter sizes considered - along with fit to kpi of best filter. [Hover over points to see Filters!]
       """.stripMargin,
      "Auto Tuned Filter")

    val maxQualityDrops = 20

    val filter = ArrayBuffer[String]()
    val precision = ArrayBuffer[Double]()
    val recall = ArrayBuffer[Double]()
    val score = ArrayBuffer[Double]()
    val sizes = ArrayBuffer[Double]()

    var bestScore = 0.0
    var bestSize: Int = 0

    var k = 1
    var drops = 0

    var analyzed = scala.collection.mutable.Set[String]()

    while (k < features.length + 1 && drops <= maxQualityDrops) {

      val featureSet = features.slice(0, k)
      val intentFilter = FilterModel.featuresToFilter(featureSet)
      println("*** Testing Filter :" + intentFilter + " ***")
      if (!(analyzed contains intentFilter.getOrElse("TotalConsumer"))) {
        val entityDof = if (entityPenalty) math.log(k + 1) else 0.0
        val (currentRsquare, coverage, _) = evaluateFilter(intentFilter, entityDof, verbosity = 0)
        val currentScore = math.sqrt(coverage) * currentRsquare / (1 - currentRsquare)
        if (currentScore >= bestScore) {
          drops = 0
          bestScore = currentScore
          bestSize = k
        } else if (currentScore < bestScore) {
          drops += 1
        }
        analyzed += intentFilter.getOrElse("TotalConsumer")
        filter.append(intentFilter.getOrElse("TotalConsumer"))
        precision.append(currentRsquare)
        recall.append(coverage)
        score.append(currentScore)
        sizes.append(k)
      }
      k += 1
    }

    val escapedFilter = filter.map(x => x.replaceAll("'", raw"\\'"))

    output.get.scatter(escapedFilter, "Terms",
      Seq("Adjusted R Square" -> precision, "Avg Counts" -> recall), sigfigs = 3, title = Some("Precision (R^2) vs. Recall (Avg User Counts)"))

    output.get.scatter(escapedFilter, "Terms",
      Seq("# of Terms" -> sizes, "Precision (R^2)" -> precision), sigfigs = 3, title = Some("Number of Terms vs. R^2"), logPlot = false)

    output.get.scatter(escapedFilter, "Terms",
      Seq("# of Terms" -> sizes, "Recall" -> recall), sigfigs = 3, title = Some("Number of Terms vs. Coverage"), logPlot = false)

    output.get.scatter(escapedFilter, "Terms",
      Seq("# of Terms" -> sizes, "Score" -> score), sigfigs = 3, title = Some("Number of Terms vs. Combined Score"), logPlot = false)

    output.get.table(sizes.toSeq.map {
      _.toString
    }, "Filter Size", Seq(("Precision", precision.toSeq), ("Coverage", recall.toSeq), ("Combined Score", score.toSeq), ("Filters", filter.toSeq)))

    val bestFilter = FilterModel.featuresToFilter(features.slice(0, bestSize))
    val entityDof = if (entityPenalty) math.log(bestSize + 1) else 0.0
    evaluateFilter(bestFilter, entityDof, verbosity = 1)

    bestFilter
  }

  def evaluateFilter(filter: Option[String],
                     entityPenalty: Double = 0.0,
                     verbosity: Int = 0): (Double, Double, Seq[Double]) = {

    val filterCounts = clusterclient.getTimeSeries(periodsAsOffsets, filter)
    val intentfulCounts = if (metricType == "diffFromConsumer") consumerCounts.zip(filterCounts).map { case (c, x) => c - x }
    else filterCounts

    val filterStats = FilterModel.bootstrappedFit(model, intentfulCounts, preProcess, kpiValues, 5000, percentValidation, entityPenalty,resampleCounts)

    val rsquared = filterStats("Adjusted R Squared").mean
    val coverage = intentfulCounts.sum / intentfulCounts.length

    val a = filterStats("a").mean
    val b = filterStats("b").mean
    val qpi = model.prediction(preProcess.process(intentfulCounts), (a, b))


    if (verbosity > 0) {

      val statnames = filterStats.trackedList.toSeq
      val escapedFilter = Some(filter.getOrElse("").replaceAll("'", raw"\\'"))
      val xrange = periods.map { x => x._2.toYearMonthDay.toString }
      output.get.plot(xrange, "periods", Seq(("kpi", kpiValues), ("qpi", qpi)), sigfigs = 3, title = escapedFilter)

      output.get.table(Seq("Filterful", "Consumer"), "", Seq(("Avg Counts per Period", Seq(intentfulCounts.sum / intentfulCounts.length, consumerCounts.sum / consumerCounts.length))))
      output.get.table(statnames, "Fit Statistics",
        Seq(("Value", statnames.map {filterStats(_).mean}),
          ("CI Min", statnames.map {filterStats(_).head()._1}),
          ("CI Max", statnames.map {filterStats(_).head()._2})), 5)
    }

    if (verbosity > 1) {

      val xrange = periods.map { x => x._2.toYearMonthDay.toString }
      val filterComponents = filter.map {splitQuery}
      val rows = filterComponents.getOrElse(Seq("TotalConsumer")).map { k => (k, preProcess.process({
        if (k == "TotalConsumer") consumerCounts else clusterclient.getTimeSeries(periodsAsOffsets, Some("(" + k + ")"))
      }))}.sortBy {_._2.sum}
      rows.grouped(6).foreach { r => output.get.plot(xrange, "periods", r, sigfigs = 3, title = Some("Entity Counts (Rescaled)")) }

      if (verbosity > 3) {

          val grouping = xrange.length/20
          output.get.heatMap(rows.map{case (e,ts) => (e, scoreTimeByDropping(model.similarity, ts, kpiValues, 0.2, 1000).grouped(grouping).map{_.sum}.toSeq)}, "terms", xrange.grouped(grouping).map{_.head}.toSeq,2,0.25)

          val consumerCorr = crossCorrelation(preProcess.process(consumerCounts), kpiValues, 12)
          val laggedCorr = rows.toMap.mapValues { x => crossCorrelation(x, kpiValues, 12) }

          output.get.heatMap(Seq(("TotalConsumer", consumerCorr)) ++ laggedCorr.toSeq, "terms", (0 to 12).toSeq.map {_.toString})
        }
      }

    (rsquared, coverage, qpi)
  }
}

object FilterModel {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println(t1 - t0)
    result
  }

  def featuresToFilter(features: Seq[String]): Option[String] = {
    /* Total Consumer ==> No Filter */
    if ((features contains "TotalConsumer") || features.isEmpty) {
      None
    } else if (features.length == 1) {
      /* Just a single term */
      Some("(" + features.head + ")")
    } else {
      /* Split up clusters (stored as comma seperated terms) */
      val individualFeatures = features.flatMap {
        _.split(",(?=([^`]*`[^`]*`)*[^`]*$)", -1)
      }
      /* Buffer for distinct terms */
      val reduced = ArrayBuffer[String]()
      for (entity <- individualFeatures.sortWith(_.length < _.length)) {
        /* Strip away ` for escaping terms - and check if n-gram is already covered by a present m gram */
        val contained = reduced.foldLeft(false) { (b, s) => b | (" " + entity.replaceAll("`", "") + " " contains " " + s.replaceAll("`", "") + " ") }
        if (!contained) reduced += entity
      }
      Some("(" + reduced.mkString(",") + ")")
    }
  }

  /* ================================ FWER ===================================== */

  def normalizeMetric(CountsMap: Map[String, IndexedSeq[Double]],
                      kpi: Seq[Double],
                      metric: (Seq[Double], Seq[Double], Option[Seq[Double]]) => Double,
                      preProcess: preProcess[Double],
                      iters: Int,
                      gviz: Option[GoogleChart] = None): (Double, Double) = {

    import com.qf.util.conversions.collection._
    import scala.concurrent.ExecutionContext.Implicits.global

    val threads = 8
    val D = CountsMap.size
    val terms = CountsMap.keys.toIndexedSeq
    val itersPerThread = iters / threads
    val out: List[Seq[Double]] =
      (1 to 8).toList.pmap { t =>
        Seq.tabulate(itersPerThread) { i =>
          var max: Double = 0.0
          for (j <- 1 to D) {
            val x = metric(preProcess.process(randomTimeSeries(terms, CountsMap, kpi.length)), kpi, None)
            if (x > max) max = x
          }
          max
        }
      }

    val (mean, variance) = getStats(out.flatten)

    if (gviz.isDefined) {
      val sortedStats = out.flatten.sorted
      println(sortedStats.mkString(","))
      val binSize = (sortedStats.max - sortedStats.min) / 20.0
      val binEdges = sortedStats.min to sortedStats.max by binSize

      val indices = binEdges.map { v => sortedStats.indexWhere(v < _) }.toSeq.map { x => if (x == -1) sortedStats.length else x }
      val binCounts = indices.drop(1).zip(indices).map { case (a, b) => (a - b).toDouble }
      val binNames = binEdges.drop(1).zip(binEdges).map { case (a, b) => b.toString + "-" + a.toString }
      gviz.get.plot(binNames, "Bins", Seq(("Counts", binCounts)))
    }

    (mean, math.sqrt(variance))
  }

  def sidac(pValue: Double, totalTests: Int): Seq[Double] = {
    Seq(1.0 - math.pow(1 - pValue, 1.0 / totalTests))
  }

  def sidacHolm(pValue: Double, totalTests: Int, maxSelected: Int): Seq[Double] = {
    Seq.tabulate(maxSelected * 5) { i => 1 - math.pow(1 - pValue, 1.0 / (totalTests - i)) }
  }

  /** =================== False Discovery Rate ================= **/

  /* https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg.E2.80.93Yekutieli_procedure */
  def BHYProcedure(rate: Double, totalTests: Int, maxSelected: Int): Seq[Double] = {
    val c = 0.5772156649 + math.log(totalTests)
    val x = rate/(totalTests*c)
    Seq.tabulate(5*maxSelected){i => (i+1)*x}
  }

   /* https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg.E2.80.93Yekutieli_procedure */
  def BHProcedure(rate: Double, totalTests: Int, maxSelected: Int): Seq[Double] = {
    val x = rate/totalTests
    Seq.tabulate(5*maxSelected){i => (i+1)*x}
  }

  /** Generate Random Time Series by sampling from full Date-Term Matrix. This we use as our
    * random case.
    *
    * @param terms
    * @param DateEntity
    * @param length
    * @return
    */
  def randomTimeSeries(terms: IndexedSeq[String],
                       DateEntity: Map[String, IndexedSeq[Double]],
                       length: Int,
                       dateShuffle: Boolean = true): Seq[Double] = {

    val dima = terms.length
    val dimb = DateEntity.head._2.length

    val data  = if (dateShuffle) {
      for (i <- 0 to length - 1) yield {
        val j = (dima * math.random).toInt
        val k = (dimb * math.random).toInt
        DateEntity(terms(j))(k)
      }
    } else {
      for (i <- 0 to length - 1) yield {
        val j = (dima * math.random).toInt
        DateEntity(terms(j))(i)
      }
    }
    data.toSeq
  }

  /** Rank terms using some metric - typically normalized correlation
    *
    * @param x : Counts data as map String->Seq[Double]
    * @param y : KPI as Seq[Double]
    * @param topk : Number of terms to return.
    * @param corrType : Function used to compare time-series
    * @return : Sequence of pairs (entity, correlation)
    */
  def rankTerms(x: Map[String, Seq[Double]],
                   y: Seq[Double],
                   corrType: (Seq[Double], Seq[Double], Option[Seq[Double]]) => Double,
                   preProcess: preProcess[Double],
                   corrThresh: Double = 0.0,
                   training: Option[Seq[Double]],
                   topk: Int,
                    countsResample: Boolean = true): Seq[Seq[(String, Double)]] = {

    val correlations = if (countsResample)  x.mapValues { v => corrType(preProcess.process(resample(v)), y, training) }.toSeq
                          else x.mapValues { v => corrType(preProcess.process(v), y, training) }.toSeq
    val ranked = correlations.sortWith {
      _._2 > _._2
    }
    val rankings = ranked.map {
      _._1
    }.zipWithIndex.map { case (e, v) => (e, v + 1.0) }
    Seq(correlations, rankings, ranked.slice(0, topk).filter {
      _._2 > corrThresh
    })
  }
  /* =============================== FIT TO KPI ================================== */

  /** Run numsamples iterations of fitting counts time-Series to KPI with fluctuations introduced to randomly removing
    * dates from training set and including error in counts
    *
    */
  def bootstrappedFit(fitMethod: TimeSeriesModel[Double, (Double, Double)],
                      countData: Seq[Double],
                      preProcess: preProcess[Double],
                      kpiValues: Seq[Double],
                      numsamples: Int = 50,
                      evalPercent: Double = 0.2,
                      entityPenalty: Double = 0.0,
                      resampleCounts: Boolean = true): StatTracker = {

    bootStrapIt(numsamples, 0.95) { x =>
      val (training, evaluation) = trainEvalSplit(kpiValues.length, evalPercent)
      if (resampleCounts) fitToKPI(fitMethod, preProcess, kpiValues, (Some(training), Some(evaluation)), resample(countData), entityPenalty)
      else fitToKPI(fitMethod, preProcess, kpiValues, (Some(training), Some(evaluation)), countData, entityPenalty)
    }
  }

  /** Performs the linear fit of intent counts to KPI - outputing various metrics for goodness of fit.
    *
    * @param fitMethod
    * @param preProcess
    * @param kpiValues
    * @param trainEvalSplit
    * @param counts
    * @param entityPenalty
    * @return
    */
  def fitToKPI(fitMethod: TimeSeriesModel[Double, (Double, Double)],
               preProcess: preProcess[Double],
               kpiValues: Seq[Double],
               trainEvalSplit: (Option[Seq[Double]], Option[Seq[Double]]) = (None, None),
               counts: Seq[Double],
               entityPenalty: Double): Seq[(String, Double)] = {

    if (trainEvalSplit ==(None, None)) {
      val processedCounts = preProcess.process(counts)
      val (a, b) = fitMethod.regression(processedCounts, kpiValues, None)
      val qpi = fitMethod.prediction(processedCounts, (a, b))

      Seq(("a", a),
        ("b", b),
        ("RMSE", rmse(qpi, kpiValues)),
        ("MAPE", mape(qpi, kpiValues)),
        ("Adjusted R Squared", fitMethod.rsquareAdjusted(qpi, kpiValues, qpi.length, 1 + entityPenalty)))
    }
    else {
      val train = trainEvalSplit._1
      val eval = trainEvalSplit._2
      val processedCounts = preProcess.process(counts)
      val (a, b) = fitMethod.regression(processedCounts, kpiValues, train)
      val qpi = fitMethod.prediction(processedCounts, (a, b))

      Seq(("a", a),
        ("b", b),
        ("Training RMSE", rmse(qpi, kpiValues, train)),
        ("Evaluation RMSE", rmse(qpi, kpiValues, eval)),
        ("MAPE", mape(qpi, kpiValues)),
        ("Evaluation MAPE", mape(qpi, kpiValues, eval)),
        ("Adjusted R Squared", fitMethod.rsquareAdjusted(qpi, kpiValues, kpiValues.length, 1 + entityPenalty)),
        ("Evaluation Adj. R Squared", fitMethod.rsquareAdjusted(qpi, kpiValues, kpiValues.length, 1 + entityPenalty, eval)))
    }
  }
}

/**
  *
  * numFeatures: Number of terms in filter
  * numsamples: Number of bootstrap/jacknife iterations
  * percentValidation: What fraction of data to drop from training set per iteration.
  *
  * corrType = metric used to rank entitites
  * Options:
  * 1) corrType = FilterModel.normalizedCorr
  *     Normalized correlation of series Cor(x,y)/sqrt(Var(x)Var(y))
  * 2) corrTYpe = FilterModel.linfit(_,_)._1
  *     Coefficient of linear fit
  * 3) whatever else you can think of...
  *
  * outputDir: Results are output to a directory - including stats, plots, sample tweets, etc.
  * fromFile: Once the date-entity has been downloaded - option to use local data.
  * consumerScaling: Use consumer time series to rescale entity counts.
  **/

object FilterModule extends ArgMain[FilterModuleArgs] {

  def main(args: FilterModuleArgs): Unit = {

    val output = new GoogleChart

    /* MAKE SURE OUTPUT DIR EXISTS */
    val dir = new File(args.outputDir)
    if (!dir.exists) dir.mkdir

    /* CONFIGURE Cluster CLIENT USING THE GIVEN QPID, VERTICAL, CONFIG, EXTRA FILTER, ETC */
    val clusterclient = ConfiguredClusterClient(args.host, args.port, logging = true, args.qpid, args.qpidName, args.vertical, args.outputDir, args.dateEntityFromFile,
      Option(args.expressionFilter), args.tags, Option(args.preFilter), Option(args.stopTerms), filterEmoticons = args.filterEmoticons, Option(args.configFile))

    /* CONFIGURE NEW END DATE - BASED ON SAMPLE DATA AVAILABILITY */
    val end = new DateTime(args.endDate)
    val modelEndDate = if (args.scalingType == "sample") {
      val samplesEnd = clusterclient.sampleStopDate()
      if (samplesEnd isAfter end) end else samplesEnd
    } else end

    println("*** Importing KPI ***")
    val fullKpi = importKPI(args.qpid, new DateTime(args.startDate), end, args.timeLag, Option(args.kpiFile), args.kpiColumn,
      args.delimiter, args.dateFormat, args.endDateOnly, args.kpidb.database)
    val fullKpiValues = fullKpi.map { x => x._3 }
    if (args.seasonalDecomp) println("*** Reducing KPI ***")
    val kpiValues = if (args.seasonalDecomp) Rdecompose(fullKpiValues, args.period)._3 else fullKpiValues
    val periods = fullKpi.map { x => (x._1, x._2) }
    val kpiData = periods.zip(kpiValues).map { case ((s, e), v) => (s, e, v) }

    val rankedModel = new FilterModel(clusterclient, kpiData, args.corrType, args.preProcess, args.scalingType, args.holdOutSamples, args.includeTotalConsumers,
      args.outputDir, args.dateEntityFromFile, args.clusteringThreshold, args.maxFeatures, args.aboveRandom, args.bootstrapIterations,
      args.percentValidation, args.resampleCounts, args.verbose, Some(output))

    val parametersTable = new HtmlTable("Parameters",
      Map("" ->
        Seq("PreProcess","Scaling", "Bootstrap Iterations", "Percent Dates in Evaluation", "Max Terms",
          "Clustering Threshold", "p Value For Ranking", "Fit Type"),
        "Values" -> Seq(args.preProcess, args.scalingType, args.bootstrapIterations, args.percentValidation, args.maxFeatures,
          args.clusteringThreshold, args.aboveRandom, args.corrType)))
    output.addHtmlTable(parametersTable)

    output.text( s"""Terms ranked by comparison to kpi using ${args.corrType} metric.""", "Entity Ranking:")
    /* Entity Selection By Comparison With Kpi */
    val topFeatures = rankedModel.selectTermsByRanking(args.maxFeatures, args.bootstrapIterations)

    rankedModel.tuneFilterSize(topFeatures, args.entityPenalty)

    output.text( s"""Filter using maximum set of terms that were above the random case threshold""", "Full Filter")

    val intentFilter = if (topFeatures contains "TotalConsumer") None else FilterModel.featuresToFilter(topFeatures)
    val entityDof = if (args.entityPenalty) math.log(topFeatures.length + 1) else 0.0
    val (rsq, cov, qpi) = rankedModel.evaluateFilter(intentFilter, entityDof, verbosity = args.verbose)

    /* Get sample tweets */

    for (topFeature <- topFeatures) {
      val featureSamples = clusterclient.getTweetSamples(Seq(topFeature))
      val limitedFeatureSamples = featureSamples.slice(0, ArrayBuffer(20, featureSamples.size).min)
      output.table(limitedFeatureSamples.map {
        s => StringEscapeUtils.escapeHtml4(s.txt).reverse.dropWhile(_ == '\\').reverse
      }, "Sample Tweets: " + topFeature, Seq(), height = Some(300))
    }

    val mapper = ScalaJsonFactory.getObjectMapper()
    val qpidString = if (args.qpid.length > 50) "aggregated" else args.qpid

    val timestamp = new DateTime()
    val filterfile = new PrintWriter(new BufferedWriter(new FileWriter(args.outputDir + "/" + qpidString + timestamp.getMillis.toString + ".filter")))

    val filterRecord = if (Option(args.kpiFile) == None) {
      FilterRecord(args.qpid, args.qpidName, Option(args.preFilter), args.vertical, intentFilter.getOrElse("TotalConsumer"), args.scalingType, args.kpiFile, 0, "NA", "NA", args.endDateOnly)
    } else {
      FilterRecord(args.qpid, args.qpidName, Option(args.preFilter), args.vertical, intentFilter.getOrElse("TotalConsumer"), args.scalingType,
        args.kpiFile, args.kpiColumn, args.delimiter, args.dateFormat, args.endDateOnly)
    }
    mapper.writeValue(filterfile, filterRecord)
    filterfile.close()

    val qpiFile = new FileWriter(args.outputDir + "/" + qpidString + timestamp.getMillis.toString + "_qpi.csv")
    kpiData.zip(qpi).foreach { case ((s, e, k), q) => qpiFile.write(s"${s.toYearMonthDay},${e.toYearMonthDay},$k,$q\n") }
    qpiFile.close()

    output.toFile(args.outputDir + "/" + qpidString + "_" + args.corrType + "_" + timestamp.getMillis.toString + "_viz.html")

    clusterclient.shutdown()
  }
}


object CredibilityModule extends ArgMain[CredibilityModuleArgs] {

  def main(args: CredibilityModuleArgs): Unit = {

    val output = new GoogleChart

    val mapper = ScalaJsonFactory.getObjectMapper()
    val record = if (Option(args.filterFile).isDefined) mapper.readValue(new java.io.File(args.filterFile), classOf[FilterRecord])
                 else FilterRecord(args.qpid, args.qpidName, Some(args.preFilter), args.vertical, args.filter, args.scalingType,
                                   args.kpiFile, args.kpiColumn, args.delimiter, args.dateFormat, args.endDateOnly)

    val clusterclient = ConfiguredClusterClient(args.host, args.port, args.clusterLogging, record.qpid, record.qpidName, record.vertical, args.outputDir, dateEntityFromFile = false,Option(args.expressionFilter),
      args.tags, Option(args.preFilter), Option(args.stopTerms), filterEmoticons = args.filterEmoticons, Option(args.configFile))

    val kpi = importKPI(record.qpid, new DateTime(args.startDate), new DateTime(args.endDate), args.timeLag,
        Option(record.kpiFile), record.kpiColumn, record.delimiter, record.dateFormat, record.endDateOnly, args.kpidb.database)
    val periods = kpi.map{case (s,e,v) => (s,e)}
    val periodsAsOffsets =datePeriodToOffsets(periods)

    val rankedModel: FilterModel = new FilterModel(clusterclient, kpi, args.corrType, args.preProcess, args.scalingType, 0, false, args.outputDir, false, 1.01, 50, 0.1, 500, args.percentValidation, args.resampleCounts, 3, Some(output))

    val intentFilter = Some(record.filter)
    val entityDof = 0.0
    val (rsq, cov, qpi) = rankedModel.evaluateFilter(intentFilter, entityDof, verbosity = 2)

    val qpidString = if (record.qpid.length > 50) "aggregated" else record.qpid
    val dir = new File(args.outputDir)
    if (!dir.exists) dir.mkdir

    val timestamp = new DateTime()
    val qpiFile = new FileWriter(args.outputDir + "/" + qpidString + timestamp.getMillis.toString +"_qpi.csv")
    kpi.zip(qpi).foreach{case ((s,e,k),q) => qpiFile.write(s"${s.toYearMonthDay},${e.toYearMonthDay},$k,$q\n")}
    qpiFile.close()

    if(args.topicSensitivity)
      Credibility.evaluateTopics(clusterclient, kpi, Some(record.filter), output, rankedModel.scaling, rankedModel.consumerCounts, args.corrType)

    if(args.demoSensitivity)
      Credibility.evaluateDemos(clusterclient, kpi, Some(record.filter), output, rankedModel.scaling, rankedModel.consumerCounts, args.corrType)

    output.toFile(args.outputDir+"/"+qpidString+"_"+ timestamp.getMillis.toString +"_credibility.html")

    clusterclient.shutdown()
  }
}

case class FilterRecord(
                         qpid: String,
                         qpidName: String,
                         preFilter: Option[String] = None,
                         vertical: String,
                         filter: String,
                         scalingType: String,
                         kpiFile: String,
                         kpiColumn: Int,
                         delimiter: String,
                         dateFormat: String,
                         endDateOnly: Boolean)

class FilterModuleArgs extends kpiArgs with KpidDBProvided with ClusterClientArgs with configClusterArgs with RandomizationArgs{
  var configFile: String = _
  var qpid: String = _
  var qpidName: String = _
  var vertical: String = _
  var startDate: String = _
  var endDate: String = _
  var holdOutSamples: Int = 0
  var maxFeatures: Int = _
  var corrType: String = "correlation"
  var scalingType: String = "spritzerSample"
  var clusteringThreshold: Double = 100.0
  var aboveRandom: Double = 0.1
  var entityPenalty: Boolean = false
  var preProcess: String = _
  var verbose: Int = 3
}

class CredibilityModuleArgs extends kpiArgs with KpidDBProvided with ClusterClientArgs with configClusterArgs with RandomizationArgs{
  var configFile: String = _
  var filterFile: String = _
  var qpid: String = _
  var qpidName: String = _
  var vertical: String = _
  var startDate: String = _
  var endDate: String = _
  var scalingType: String = "spritzerSample"
  var filter: String = _
  var corrType: String = "correlation"
  var preProcess: String = "scaling"
  var demoSensitivity: Boolean = true
  var topicSensitivity: Boolean = true
}

trait RandomizationArgs {
  var bootstrapIterations: Int = 500
  var percentValidation: Double = 0.2
  var resampleCounts: Boolean = true
}

trait configClusterArgs {
  var preFilter: String = _
  var filterEmoticons: Boolean = false
  var tags: Option[String] = None
  var expressionFilter: String = _
  var stopTerms: String = _
  var clusterLogging: Boolean = true
  var includeTotalConsumers: Boolean = false
  var dateEntityFromFile: Boolean = false
  var outputDir: String = _
}

trait kpiArgs extends EnvArgs {
  var timeLag: Int = 0
  var kpiFile: String = _
  var kpiColumn: Int = _
  var delimiter: String = _
  var dateFormat: String = _
  var endDateOnly: Boolean = false
  var seasonalDecomp: Boolean = false
  var period: Int = _
}
