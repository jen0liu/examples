package com.qf.science.intent

import java.io.File
import java.util.zip.ZipInputStream

import com.qf.config.{PartitionDefinition, SQLArgs}
import com.qf.rtc._
import com.qf.science.MatrixUtils
import com.qf.science.kpiparsers.{FinancialSanitizer, FinancialSanitizerConfiguration}
import com.qf.util.DatePeriod
import com.qf.util.LogicTree
import org.apache.http.HttpEntity

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import java.net.{URLEncoder}
import org.joda.time.{DateTime,format,Duration}
import com.github.nscala_time.time.Imports._
import org.saddle.{Series}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.slick.driver.PostgresDriver

import com.qf.signum.SignumConfig
import com.qf.json.ScalaJsonFactory

/**
 * Created by adam on 9/26/14.
 */

object ConfiguredRTCClient {

  def apply(rtchost: String,
           rtcport: Int,
           logging: Boolean = false,
           qpid: String,
           qpidName: String,
           vertical: String,
           outDir: String,
           dateEntityFromFile: Boolean = false,
           expressionFilter: Option[String],
           tags: Option[String],
           preFilter: Option[String],
           stopTerms: Option[String],
           filterEmoticons: Boolean = false,
           configFile: Option[String]) = {

    val mapper = ScalaJsonFactory.getObjectMapper()

    val config = configFile.map(f => mapper.readValue(new java.io.File(f), classOf[SignumConfig]))

    val stopTermsSet = stopTerms.getOrElse("").split(",").map{_.trim}.toSet
    println(stopTermsSet)

    val blacklistFilter = config.flatMap(c => {



      //val extraFilters = c.resolvedQpidEntityDefinitions.values.filter(_.qpids.contains(qpid)).map(_.extraFilter).toSeq
      val extraFilter = c.resolvedQpidEntityDefinitions(qpidName).extraFilter

      val genExtraFilter = c.resolvedQpidEntityDefinitions(qpidName).genExtraFilter.flatMap(_.terms)
      //val extraFilters = c.resolvedQpidEntityDefinitions.values.filter(_.genQpidsFilter.get.qpids.exists(_ == qpid)).map(_.genExtraFilter.get.terms).toSeq

      //    println(s"Extra Filter $extraFilters")
      genExtraFilter.orElse(extraFilter).map(_.toString)
    })

    new ConfiguredRTCClient(rtchost, rtcport, logging, qpid, qpidName, vertical, outDir, dateEntityFromFile,
      blacklistFilter, expressionFilter, tags, preFilter, stopTermsSet, filterEmoticons, config.map{_.topicMap},
      config.map{_.demoPartitionMapMap(qpidName)})
  }
}

class ConfiguredRTCClient (rtchost: String,
                           rtcport: Int,
                           logging: Boolean,
                           qpid: String,
                           qpidName: String,
                           vertical: String,
                           outDir: String,
                           dateEntityFromFile: Boolean = false,
                           blacklistFilter: Option[String] = None,
                           expressionFilter: Option[String] = None,
                           tags: Option[String] = None,
                           preFilter: Option[String] = None,
                           stopTerms: Set[String] = Set(),
                           filterEmoticons: Boolean = false,
                           topics: Option[Map[String, String]] = None,
                           demos: Option[Map[String, Map[String, PartitionDefinition]] ] = None) {

  val rtcclient = new RTCClient(rtchost, rtcport,maxBatchSize = 10, logging = logging)
  val topicMap:Option[Map[String,String]] = topics
  val demoMap:Option[Map[String, Map[String, PartitionDefinition]]] = demos

  def getDateEntity (periods: Seq[(DateTime, DateTime)], forVertical: Boolean = false): Map[String, IndexedSeq[Double]] = {

    println("*** Importing Term-Date Matrix ...")

    val qpidString = if (forVertical) "entireVertical" else if (qpid.length > 50) "aggregated" else qpid
    if (!dateEntityFromFile) {

      val urlstring = "termCounts" +
        "?targetVertical=" + vertical +
        {if (!forVertical) "&targetQpids=" + qpid else ""} +
        "&groupBy=date" +
        {if (blacklistFilter.isEmpty) "" else {"&targetTerms=" + URLEncoder.encode(blacklistFilter.get,"UTF-8")}} +
        {if (preFilter.isEmpty) "" else {"&targetPrefilterVertical=" + vertical + "&targetPrefilterQpids=" + URLEncoder.encode(preFilter.get,"UTF-8")}} +
        {if (expressionFilter.isEmpty) "" else {"&targetExpr=" + URLEncoder.encode(expressionFilter.get,"UTF-8")}} +
        {if (tags.isEmpty) "" else {"&targetTags=" + URLEncoder.encode(tags.get, "UTF-8")}} +
        "&termFilterType=WordLike"

      val httpclient = new DefaultHttpClient()
      val url = s"http://$rtchost:$rtcport/" + urlstring
      println("executing: " + url)
      val request = new HttpGet(url)
      val response = httpclient.execute(request)
      val httpEntity: HttpEntity = response.getEntity
      val stream = httpEntity.getContent

      val zipStream = new ZipInputStream(stream)

      val dir = new File(outDir + "/" + qpidString)
      if (!dir.exists) dir.mkdir

      var entry = zipStream.getNextEntry()
      while (entry != null) {
        val outpath = outDir + "/" + qpidString +"/" + entry.getName()
        val in = Source.fromInputStream(zipStream)
        val f = new File(outpath)
        f.createNewFile()
        val out = new java.io.PrintWriter(f)
        in.getLines().foreach(out.println(_))
        out.flush
        out.close
        entry = zipStream.getNextEntry()
      }
    }
    var DateEntity = MatrixUtils.fetchDataFrameFromMatrixMarket(outDir + "/"+qpidString + "/results", dateAsString=true).toColSeq
    val DateEntityStopRemoved = if (filterEmoticons) DateEntity.filter{x => !(stopTerms contains x._1) && !("[\u2700-\u27bf\ud83c\udf00-\ud83d\ude4f\ud83d\ude80-\ud83d\udef3]".r.pattern.matcher(x._1).find())}
        else DateEntity.filter{x => !(stopTerms contains x._1)}

    println("... Import Complete ***")
    DateEntityStopRemoved.map {x => (x._1,DataInput.groupByDate(periods, x._2))}.toMap
  }

  def getTweetSamples(features: Seq[String]): Seq[SampledComment] = {

    val samples = rtcclient.call(features.map {case x =>
      SampleRequest(RequestGroup(
        source = "twitter",
        vertical = vertical,
        qpids=Seq(qpid),
        searchTerms = Some("(" + x + ")"),
        tags = tags
      ))
    })
    samples.foldLeft(Seq[SampledComment]())((a,b)=>a++b)
  }

  def getTimeSeries(periodsAsOffsets: (DateTime,DateTime,Seq[Int]), terms: Option[String]=None): Seq[Double] = {

    val targetGroup = RequestGroup(vertical = vertical, qpidsTree = preFilter)
    val combinedTerms: Option[String] = LogicTree.andOptions(terms, blacklistFilter).map(_.toString)

    val timeSeries = rtcclient.call(
      DatePeriodCountsRequest(
        vertical = vertical,
        qpidsTree = Some(qpid),
        preFilter = if (preFilter.isEmpty) None else Some(targetGroup),
        expressions = expressionFilter,
        startDate = Some(periodsAsOffsets._1),
        endDate = Some(periodsAsOffsets._2),
        dateOffsets = periodsAsOffsets._3,
        searchTerms = combinedTerms,
        tags = tags))

      timeSeries.head.counts.map{_.count}
  }

  def getTopicStrengthScores(periodsAsOffsets: (DateTime,DateTime,Seq[Int]), intentTerms: Option[String], topicTerms: Option[String]): Seq[Double] = {

    val topicTimeSeries = getTopicTimeSeries(periodsAsOffsets, intentTerms, topicTerms)
    val intentTimeSeries = getTimeSeries(periodsAsOffsets, intentTerms)

    topicTimeSeries.zip(intentTimeSeries).map {
      case (topicAndIntentCounts, intentfulCounts) =>  if (intentfulCounts==0) 0.0 else topicAndIntentCounts/intentfulCounts
    }
  }

  def getTopicTimeSeries(periodsAsOffsets: (DateTime,DateTime,Seq[Int]), intentTerms: Option[String], topicTerms: Option[String]): Seq[Double] = {

    val targetGroup = RequestGroup(vertical = vertical, qpidsTree = preFilter)
    var combinedTerms: Option[String] = LogicTree.andOptions(intentTerms, blacklistFilter).map(_.toString)
    combinedTerms = LogicTree.andOptions(combinedTerms, topicTerms).map(_.toString)

    val timeSeries = rtcclient.call(
      DatePeriodCountsRequest(
        vertical = vertical,
        qpidsTree = Some(qpid),
        expressions = expressionFilter,
        startDate = Some(periodsAsOffsets._1),
        endDate = Some(periodsAsOffsets._2),
        dateOffsets = periodsAsOffsets._3,
        tags = tags,  
        preFilter = if (preFilter.isEmpty) None else Some(targetGroup),
        searchTerms = combinedTerms))

    timeSeries.head.counts.map{_.count}
  }

  def getDemoPartitionTimeSeries(periodsAsOffsets: (DateTime,DateTime,Seq[Int]), terms: Option[String], demoPartition: PartitionDefinition): Seq[Double] = {

    val targetGroup = RequestGroup(vertical = vertical, qpidsTree = preFilter)
    val combinedTerms: Option[String] = LogicTree.andOptions(terms, blacklistFilter).map(_.toString)

//    println("Now")
//    println(demoPartition.definition)
//    val expressionFilter = Some("&targetExpr=" + URLEncoder.encode(demoPartition.definition,"UTF-8"))
//    println(expressionFilter.toString)

    val timeSeries = rtcclient.call(
      DatePeriodCountsRequest(
        vertical = vertical,
        qpidsTree = Some(qpid),
        expressions = Some(demoPartition.definition),
        startDate = Some(periodsAsOffsets._1),
        endDate = Some(periodsAsOffsets._2),
        dateOffsets = periodsAsOffsets._3,
        tags = tags,
        preFilter = if (preFilter.isEmpty) None else Some(targetGroup),
        searchTerms = combinedTerms))

    timeSeries.head.counts.map{_.count}
  }

  def getVerticalTimeSeries(periodsAsOffsets: (DateTime,DateTime,Seq[Int]), terms: Option[String]=None): Seq[Double] ={

    val targetGroup = RequestGroup(vertical = vertical, qpidsTree = preFilter)

    val combinedTerms: Option[String] = {
      (terms, blacklistFilter) match {
        case (None, None) => None
        case (Some(ent), None) => Some(ent)
        case (None, Some(bla)) => Some(bla)
        case (Some(ent), Some(bla)) => Some("[" + ent + "," + bla + "]")
      }
    }

    val timeSeries = rtcclient.call(
      DatePeriodCountsRequest(
        vertical = vertical,
        qpidsTree = None,
        preFilter = if (preFilter == None) None else Some(targetGroup),
        expressions = expressionFilter,
        startDate = Some(periodsAsOffsets._1),
        endDate = Some(periodsAsOffsets._2),
        dateOffsets = periodsAsOffsets._3,
        searchTerms = combinedTerms,
        tags = tags))

    timeSeries.head.counts.map{_.count}
  }

  def getSampleTimeSeries(periodsAsOffsets: (DateTime,DateTime,Seq[Int])): Seq[Double] = {

    val timeSeries = rtcclient.call(
      DatePeriodCountsRequest(
            vertical = "sample",
            startDate = Some(periodsAsOffsets._1),
            endDate = Some(periodsAsOffsets._2),
            dateOffsets = periodsAsOffsets._3,
            tags = tags))
    timeSeries.head.counts.map(_.count)
  }

  def getQfSampleTimeSeries(periodsAsOffsets: (DateTime, DateTime, Seq[Int])): Seq[Double] = {
    val verticals = Seq("restaurant","beverage","retail","householdproduct","candy","toy",
       "amusementpark","software","apparel","cableprovider","snack","consumerelectronics","sportsequipment",
      "cereal","ecommerce","automobile","hardwarestore","footwear","health","hotel")
      val verticalResponses = rtcclient.call(
        verticals.map{v =>
        DatePeriodCountsRequest(
          vertical = v,
          qpidsTree = None,
          startDate = Some(periodsAsOffsets._1),
          endDate = Some(periodsAsOffsets._2),
          dateOffsets = periodsAsOffsets._3,
          tags = tags)})
    val verticalTs = verticalResponses.map{_.head.counts.map{_.count}}.toSeq
    verticalTs.transpose.map{_.sum}
  }

  def sampleStopDate(): DateTime = {
    val ts = rtcclient.call(TimeSeriesRequest(source="twitter", vertical="sample"))
    new DateTime(ts(0).ts.last.date)
  }

  def getDailyTimeSeries(start: DateTime, end: DateTime, terms: Seq[String] = Seq()) = {

    val intentFilter: Option[String] = if (terms.nonEmpty) Some("(" + terms.mkString(",") + ")") else None

    val targetGroup = RequestGroup(vertical = vertical, qpidsTree = preFilter)
    
    val combinedTerms: Option[String] = {
      (intentFilter, blacklistFilter) match {
        case (None, None) => None
        case (Some(ent), None) => Some(ent)
        case (None, Some(bla)) => Some(bla)
        case (Some(ent), Some(bla)) => Some("[" + ent + "," + bla + "]")
      }
    }

    val ts = rtcclient.call(
      TimeSeriesRequest(
        vertical = vertical,
        qpidsTree = Some(qpid),
        preFilter =  if (preFilter.isEmpty) None else Some(targetGroup),
        startDate = Some(start),
        endDate = Some(end),
        searchTerms = combinedTerms,
        tags = tags))
    ts.head.ts.map{_.count}.toSeq
  }

  def shutdown() = {
    rtcclient.shutdown
  }

}

object DataInput {

  /** Seq[(DateTime,DateTime)] --> (DateTime, DateTime, Seq[Int])
    * Periods to start,end, and offsets - where negative numbers are used for gaps
    */
  def datePeriodToOffsets(periods: Seq[(DateTime,DateTime)]): (DateTime,DateTime,Seq[Int]) = {
    val start = periods.head._1
    val end = periods.last._2
    val datePeriods = periods.map{case (start,end) => DatePeriod(start,end)}
    val offsets = DatePeriod.toOffsets(datePeriods)
    (start,end,offsets)
  }

  /** Imports KPI either from file or using financial sanitizer.
    * Includes option to introduce Lag in KPI: For correlating Terms today with KPI n-months from now.
    *
    * @param qpid
    * @param start : DateTime
    * @param end   : DateTime - determines date range over which to build model
    * @param kpiLag : Int >= 0
    * @param kpiFile : String - KPI csv/tsv file
    * @param kpiColumn : Int >=1 - Which column of KPI data to extract from csv
    * @param delimiter : String - Delimiter used to separate entries in KPI file
    * @param dateFormat : String - Custom Date Form "yyyyMMdd",etc
    * @return
    */

  def importKPI (qpid: String,
                 start: DateTime,
                 end: DateTime,
                 kpiLag: Int,
                 kpiFile: Option[String],
                 kpiColumn: Int,
                 delimiter: String,
                 dateFormat: String,
                 endDateOnly: Boolean,
                 db: SQLArgs): Seq[(DateTime, DateTime, Double)] = {

    val rawKPI = if (kpiFile==None) KPIfromQpid(qpid,db)
                 else if (endDateOnly) KPIfromFileOneDate(kpiFile.get, kpiColumn, delimiter, dateFormat)
                 else KPIfromFile(kpiFile.get, kpiColumn, delimiter, dateFormat)
    val sorted = rawKPI.sortWith{_._1 isBefore _._1}
    val dates = sorted.map{case (s,e,v) => (s,e)}
    val values = sorted.map{case (s,e,v) => v}

    val shiftedDates = if (kpiLag > 0) {
      val periodSize = new Duration(dates(0)._1, dates(0)._2)
      val beginningDates = ArrayBuffer[(DateTime,DateTime)]()
      var e = dates(0)._1
      for (i <- 1 to kpiLag) {
        val s = e.minus(periodSize)
        beginningDates += ((s,e))
        e = e.minus(periodSize)
      }
      beginningDates.toSeq.reverse ++ dates.dropRight(kpiLag)
    } else if (kpiLag < 0) {
      val periodSize = new Duration(dates(0)._1, dates(0)._2)
      val endingDates = ArrayBuffer[(DateTime,DateTime)]()
      var s = dates(dates.size - 1)._2
      for (i <- 1 to kpiLag * -1) {
        val e = s.plus(periodSize)
        endingDates += ((s,e))
        s = s.plus(periodSize)
      }
      dates.drop(kpiLag * -1) ++ endingDates.toSeq
    } else {
      dates
    }

    shiftedDates.zip(values).map{case ((s,e),v) => (s,e,v)}.filter{x => !(x._1 isBefore start) && !(x._2 isAfter end)}
  }

  /**
   * Extract KPI for Qpid : Restricted to start/end dates.
   * @param qpid
   * @return
   */
  def KPIfromQpid(qpid: String, db: SQLArgs): Seq[(DateTime, DateTime, Double)] = {

    val x: Seq[(DateTime,DateTime, Double)] = FinancialSanitizerConfiguration.fromQpid(qpid) match {
      case Some((vertical, brand, kpiName)) =>
        FinancialSanitizer(vertical, brand, db.configuredDatabase(PostgresDriver)).getFinancials(kpiName).map({
          case (start,end,originalKpi,_,_,_) => (start,end,originalKpi)
        })
      case _ => throw new MatchError(s"KPIActual: $qpid not recognized")
    }
    x
  }

  /** Extract KPI from CSV/TSV File
    *
    * @param filename
    * @param kpicolumn
    * @param delimeter
    * @param customDateFormat
    * @return
    */
  def KPIfromFile(filename: String, kpicolumn: Int, delimeter: String, customDateFormat: String) : Seq[(DateTime, DateTime,Double)] = {
    val src = scala.io.Source.fromFile(filename)
    val fmt = if (Option(customDateFormat) == None || customDateFormat=="Timestamp") format.ISODateTimeFormat.dateTime()
              else format.DateTimeFormat.forPattern(customDateFormat)
    val lines = src.getLines()
    var parsed = ArrayBuffer[(DateTime,DateTime,Double)]()
    var parseError = 0
    for (line <- lines) {
      try {
        val data = line.split(delimeter)
        if (customDateFormat=="Timestamp") parsed += ((new DateTime(data(0).toLong), new DateTime(data(1).toLong), data(kpicolumn).toDouble))
        else parsed += ((fmt.parseDateTime(data(0)), fmt.parseDateTime(data(1)), data(kpicolumn).toDouble))
      } catch {
        case e: Exception => parseError += 1
      }
    }
    println("Parsing Errors: " + parseError)
    parsed.toSeq
  }

  def KPIfromFileOneDate(filename: String, kpicolumn: Int, delimeter: String, customDateFormat: String) : Seq[(DateTime, DateTime, Double)] = {

    val src = scala.io.Source.fromFile(filename)
    val fmt = if (Option(customDateFormat) == None) format.ISODateTimeFormat.dateTime()
    else format.DateTimeFormat.forPattern(customDateFormat)
    val lines = src.getLines()
    var parsed = ArrayBuffer[(DateTime,Double)]()
    var parseError = 0

    for (line <- lines) {
      try {
        val data = line.split(delimeter)
        parsed += ((fmt.parseDateTime(data(0)), data(kpicolumn).toDouble))
      } catch {
        case e: Exception => parseError += 1
      }
    }
    println("Parsing Errors: " + parseError)
    singleToDoubleDated(parsed).toSeq
  }

  def singleToDoubleDated(x: Seq[(DateTime, Double)]): Seq[(DateTime,DateTime,Double)] = {
    val dates = x.map{_._1}
    val periodSize = new Duration(dates(0), dates(1))
    val startDates = Seq(dates(0).minus(periodSize))++dates.dropRight(1)
    startDates.zip(x).map{case (s,(e,v)) => (s,e,v)}
  }


  def groupByDate(periods: Seq[(DateTime,DateTime)], datedSeries: Series[DateTime,Double]): IndexedSeq[Double] = {
    val reduced = new ArrayBuffer[Double]()
    val periodnum = periods.length

    val lastDate = periods.last._2
    val startDate = periods.head._1
    val fullTimeSeries = datedSeries.toSeq.sortWith(_._1 isBefore _._1).filter(x => (x._1 <= lastDate) && (x._1 >= startDate))
    var accum = 0.0
    var n = 0
    var period = periods(n)
    for ((d,v) <- fullTimeSeries) {
      /* If date is after current period - move to next time period*/
      while (!(d isBefore period._2) && (n<(periodnum-1))) {
        n+=1
        period = periods(n)
        reduced += accum
        accum = 0.0
      }
      /* If date inside period - add to accumulated counts */
      if (!(d isBefore period._1) && (d isBefore period._2)) {
        accum +=v
      }
    }
    reduced += accum
    n+=1
    while (n < periodnum) {
      reduced += 0.0
      n+=1
    }
    reduced.toIndexedSeq
  }
}
