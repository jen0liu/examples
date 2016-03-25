
import scala.math.{abs,sqrt}

trait supervisedModel[U,V,P] {
  def similarity(x: Seq[U], y: Seq[V], w: Option[Seq[Double]]): Double
  def regression(x: Seq[U], y: Seq[V], w: Option[Seq[Double]]): P
  def prediction(x: Seq[U], parameters: P): Seq[V]
  def toMap(parameters:P): Map[String,Double]
}

trait TimeSeriesModel[U,P] extends supervisedModel[U,Double,P] {
  def rsquare (prediction: Seq[Double], actual: Seq[Double], weight: Option[Seq[Double]] = None): Double = {
    if (weight==None) {
      val (ymean, yvar) = TimeSeriesStats.getStats(actual)
      val sqerr = TimeSeriesStats.sqErr(prediction, actual)
      1.0 - sqerr / (prediction.length * yvar)
    } else {
      val (ymean, yvar) = TimeSeriesStats.getStatsWeighted(actual, weight.get)
      val sqerr = TimeSeriesStats.sqErr(prediction,actual,weight)
      1.0 - sqerr /(weight.get.sum * yvar)
    }
  }

  def rsquareAdjusted (x: Seq[Double], y: Seq[Double], n: Int, p: Double, weight: Option[Seq[Double]] = None): Double = {
    val rsq = rsquare(x,y, weight)
    1.0 - (1.0-rsq)*(n-1.0)/(n-p-1.0)
  }
}

class leastSquares extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    TimeSeriesStats.correlation(x,y,weight)
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    TimeSeriesStats.linfit(x,y, weight)
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val (scale, offset) = parameters
    x.map { v => scale * v + offset }
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("scale"->parameters._1, "offset"->parameters._2)
  }
}

/* ========================================= POWER LAW =============== */

class powerLaw extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    TimeSeriesStats.correlation(x.map{math.log},y.map{math.log}, weight)
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    TimeSeriesStats.linfit(x.map {math.log}, y.map {math.log}, weight)
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val (exponent, scale) = parameters
    x.map { v => math.exp(scale) * math.pow(v, exponent) }
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("exponent"->parameters._1, "scale"->parameters._2)
  }
}

/* ========================================= SQUARED PERCENT ERROR =========================================================== */
/** Linear fit using squared percent error as metric
  */
class squarePercentError extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    var newWeight = y.map{v => if (v==0.0) 0.0 else 1/(v*v)}
    if (weight != None) {
      newWeight = weight.get.zip(newWeight).map { case (a, b) => a * b}
    }
    TimeSeriesStats.correlation(x,y,Some(newWeight))
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    var newWeight = y.map{v => if (v==0.0) 0.0 else 1/(v*v)}
    if (weight != None) {
      newWeight = weight.get.zip(newWeight).map { case (a,b) => a * b}
    }
    TimeSeriesStats.linfit(x,y, Some(newWeight))
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val (scale, offset) = parameters
    x.map { v => scale * v + offset }
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("scale"->parameters._1, "offset"->parameters._2)
  }
}

/* ===================================== Sobolev Style =======================================

Mixing together standard least squares which incorporates only comparison of prediction and
value at fixed times - with a term that compares the derivative of prediction and truth.
Whether or not this is USEFUL is unclear */

/* Compute D(x,y) = \sum_t (x_t-xmean)*(y_t-ymean) + lambda*(Dx_t*Dy_t) - for a particular definition of derivative */

class sobolev(lambda: Option[Double]) extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    val L = lambda match {
      case Some(v) => v
      case None => 0.5
    }
    val (xmean,xvar,ymean,yvar,cov) = sobolevSupport(L,1,x,y,weight)
    if (xvar==0 || yvar==0) 0.0 else cov / math.sqrt(xvar * yvar)
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    val L = lambda match {
      case Some(v) => v
      case None => 0.5
    }
    val (xmean,xvar,ymean,yvar,cov) = sobolevSupport(L,1,x,y,weight)
    (cov/xvar, ymean - cov/xvar*xmean)
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val (scale, offset) = parameters
    x.map { v => scale * v + offset }
  }

  def sobolevSupport (lambda: Double, filterOrder: Int, x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double,Double,Double,Double,Double) = {
    val xderiv = derivative.filteredDerivative(x, 1.0, filterOrder)
    val yderiv = derivative.filteredDerivative(y, 1.0, filterOrder)
    if (weight == None) {
      val (xmean, xvar, ymean, yvar, cov) = TimeSeriesStats.getStats(x,y)
      val (dxmean, dxvar, dymean, dyvar, dcov) = TimeSeriesStats.getStats(xderiv,yderiv)
      (xmean, (1-lambda)*xvar + lambda*dxvar, ymean, (1-lambda)*yvar+lambda*dyvar, (1-lambda)*cov+lambda*dcov)
    } else {
      val (xmean, xvar, ymean, yvar, cov) = TimeSeriesStats.getStatsWeighted(x, y, weight.get)
      val (dxmean, dxvar, dymean, dyvar, dcov) = TimeSeriesStats.getStatsWeighted(xderiv, yderiv, weight.get)
      (xmean, (1-lambda)*xvar + lambda*dxvar, ymean, (1-lambda)*yvar+lambda*dyvar, (1-lambda)*cov+lambda*dcov)
    }
  }

  def sobolevRsquare (x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {

    val corr = similarity(x, y, weight)
    corr * corr
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("scale"->parameters._1, "offset"->parameters._2)
  }
}

/* =================================================== Difference From Reference Time Series ====================== */

class diffFromReference(reference: Seq[Double]) extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    val diffFromRef = reference.zip(x).map{case (a,b) => a-b}
    TimeSeriesStats.correlation(diffFromRef, y, weight)
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    val diffFromRef = reference.zip(x).map{case (a,b) => a-b}
    TimeSeriesStats.linfit(diffFromRef, y, weight)
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val diffFromRef = reference.zip(x).map{case (a,b) => a-b}
    val (scale, offset) = parameters
    diffFromRef.map { v => scale * v + offset }
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("scale"->parameters._1, "offset"->parameters._2)
  }
}

class linearCorrected extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    val (ax,bx) = TimeSeriesStats.linfit(Seq.tabulate(x.length){_.toDouble}, x, weight)
    val xcorrected = x.zipWithIndex.map{case (v,i) => v-ax*i-bx}

    val (ay,by) = TimeSeriesStats.linfit(Seq.tabulate(y.length){_.toDouble}, y, weight)
    val ycorrected = y.zipWithIndex.map{case (v,i) => v-ay*i-by}
    TimeSeriesStats.correlation(xcorrected,ycorrected,weight)
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    val (ax,bx) = TimeSeriesStats.linfit(Seq.tabulate(x.length){_.toDouble}, x, weight)
    val xcorrected = x.zipWithIndex.map{case (v,i) => v-ax*i-bx}

    val (ay,by) = TimeSeriesStats.linfit(Seq.tabulate(y.length){_.toDouble}, y, weight)
    val ycorrected = y.zipWithIndex.map{case (v,i) => v-ay*i-by}
    TimeSeriesStats.linfit(xcorrected,ycorrected, weight)
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val (scale, offset) = parameters
    x.map { v => scale * v + offset }
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("scale"->parameters._1, "offset"->parameters._2)
  }
}

class smoothed(window: Option[Int]) extends TimeSeriesModel[Double,(Double,Double)] {
  override def similarity(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): Double = {
    TimeSeriesStats.correlation(TimeSeriesStats.rollingAverage(x,window.get),y.drop(window.get-1),weight.map{_.drop(window.get-1)})
  }

  override def regression(x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]]): (Double, Double) = {
    TimeSeriesStats.linfit(TimeSeriesStats.rollingAverage(x,window.get),y.drop(window.get-1), weight.map{_.drop(window.get-1)})
  }

  override def prediction(x: Seq[Double], parameters: (Double, Double)): Seq[Double] = {
    val (scale, offset) = parameters
    Seq.fill(window.get-1){0.0}++TimeSeriesStats.rollingAverage(x,window.get).map { v => scale * v + offset }
  }

  override def toMap(parameters: (Double,Double)) = {
    Map("scale"->parameters._1, "offset"->parameters._2)
  }

  override def rsquare (prediction: Seq[Double], actual: Seq[Double], weight: Option[Seq[Double]] = None): Double = {
    if (weight==None) {
      val (ymean, yvar) = TimeSeriesStats.getStats(actual.drop(window.get-1))
      val sqerr = TimeSeriesStats.sqErr(prediction.drop(window.get-1), actual.drop(window.get-1))
      1.0 - sqerr / (prediction.length * yvar)
    } else {
      val (ymean, yvar) = TimeSeriesStats.getStatsWeighted(actual.drop(window.get-1), weight.get.drop(window.get-1))
      val sqerr = TimeSeriesStats.sqErr(prediction.drop(window.get-1),actual.drop(window.get-1),weight.map{_.drop(window.get-1)})
      1.0 - sqerr /(weight.get.sum * yvar)
    }
  }
}

abstract class preProcess[T] {
  def process(x: Seq[T]): Seq[T]
  def dataLoss = 0
}

class identity extends preProcess[Double] {
  override def process(x: Seq[Double]) = x
}

class rescale(d: Option[Seq[Double]]) extends preProcess[Double] {
  override def process(x: Seq[Double]) = TimeSeriesStats.normalizeTimeSeries(x,d)
}

class smoothThenRescale(d: Option[Seq[Double]], windowSize: Int) extends preProcess[Double] {

  val smoothedScaling = d.map{TimeSeriesStats.rollingAverageNoLoss(_,windowSize)}
  override def process(x: Seq[Double]) = {
    TimeSeriesStats.normalizeTimeSeries(TimeSeriesStats.rollingAverageNoLoss(x,windowSize),smoothedScaling)
  }
}

class rescaleThenSmooth(d: Option[Seq[Double]], windowSize: Int) extends preProcess[Double] {

  override def process(x: Seq[Double]) = {
    TimeSeriesStats.rollingAverageNoLoss(TimeSeriesStats.normalizeTimeSeries(x,d),windowSize)
  }
}

object TimeSeriesStats {

  /* ======================= BASIC STATS ============================= */

  def mean(x: Seq[Double], weight: Option[Seq[Double]]): Double = {
    if (weight==None) {
      x.sum/x.length
    } else {
      val w = weight.get
      var m = 0.0
      for (i <- 0 to x.length-1) m+=x(i)*w(i)
      m/w.sum
    }
  }

  /** Compute mean and variance for time series
    * D = x.length
    * Xmean = 1/D Sum_i x(i)
    * Xvar = 1/D Sum_i (x(i) - Xmean)**2
    *
    * @param x
    * @return
    */
  def getStats(x: Seq[Double]): (Double, Double) = {
    var n = 0.0
    var xMean = 0.0
    var xVar = 0.0
    x.foreach {xv =>
      n = n + 1.0
      val xDelta = xv - xMean
      xMean += xDelta/n
      xVar += xDelta*(xv-xMean)
    }
    (xMean, if (n==0) 0.0 else xVar/n)
  }

  /** Mean, Variance with non-uniform weighting / measure
    * Z = Sum_i w(i)
    * Xmean = 1/Z * Sum_i (x(i)*w(i))
    * Xvar = 1/Z * Sum_i w(i)*(x(i) - Xmean)**2
    *
    * @param x
    * @param w
    * @return
    */
  def getStatsWeighted(x: Seq[Double], w: Seq[Double]): (Double, Double) = {
    require(w.length==x.length, "Require: Input Sequences must have Same Length - x:" + x.length + " -y: " + w.length)
    var total = 0.0
    var xmean = 0.0
    var xvar = 0.0
    x.zip(w).foreach{case (xv,wv) =>
      if (wv!=0.0) {
        val t = total + wv
        val xdelta = xv - xmean
        val r = xdelta * wv / t
        xmean += r
        xvar += total * xdelta * r
        total = t
      }
    }
    (xmean, if (total == 0.0) 0.0 else xvar/total)
  }

  /** Mean, variance, covariance
    *
    * @param x
    * @param y
    * @return
    */
  def getStats(x: Seq[Double], y: Seq[Double]): (Double,Double,Double,Double,Double) = {
    val dim = x.length
    require(y.length==dim, "Require: Input Sequences must have Same Length - x:" + dim + " -y: " + y.length)
    var n = 0.0
    var xmean = 0.0
    var ymean = 0.0
    var cov = 0.0
    var xvar = 0.0
    var yvar = 0.0

    x.zip(y).foreach {case (xv,yv) =>
      n = n + 1.0
      val xdelta = xv - xmean
      val ydelta = yv - ymean
      xmean = xmean + xdelta / n
      ymean = ymean + ydelta / n
      xvar = xvar + xdelta * (xv - xmean)
      yvar = yvar + ydelta * (yv - ymean)
      cov = cov + xdelta * (yv - ymean)
    }
    if (n==0) (xmean, 0.0, ymean, 0.0, 0.0)
    else (xmean, xvar/n, ymean, yvar/n, cov/n)
  }

  def getStatsWeighted(x: Seq[Double], y: Seq[Double], w: Seq[Double]): (Double,Double,Double,Double,Double) = {
    val dim = x.length
    require(y.length==dim, "Require: Input Sequences must have Same Length - x:" + dim + " -y: " + y.length)
    var total = 0.0
    var xmean = 0.0
    var ymean = 0.0
    var cov = 0.0
    var xvar = 0.0
    var yvar = 0.0

    x.zip(y).zip(w).foreach {case ((xv,yv),wv) =>
      if (wv != 0.0) {
        val t = total + wv
        val xdelta = xv - xmean
        val ydelta = yv - ymean
        val rx = xdelta * wv / t
        val ry = ydelta * wv / t
        xmean += rx
        ymean += ry
        xvar += total * xdelta * rx
        yvar += total * ydelta * ry
        cov += total * xdelta * ry
        total = t
      }
    }
    if (total==0.0) (xmean, 0.0, ymean, 0.0, 0.0)
    else (xmean, xvar/total, ymean, yvar/total, cov/total)
  }

  /** ====================================== STANDARD LEAST SQUARES ===================================== **/

  /** Pearson Correlation of two sequences
    *
    * @param x
    * @param y
    * @return
    */
  def correlation (x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]] = None): Double = {
    val (xmean,xvar,ymean,yvar,cov) = if (weight==None) getStats(x,y) else getStatsWeighted(x,y,weight.get)
    if (xvar==0 || yvar==0) 0.0 else cov/(sqrt(xvar)*sqrt(yvar))
  }

  def crossCorrelation(x: Seq[Double], y: Seq[Double], maxLag: Int): Seq[Double] = {
    for (i <- 0 to maxLag) yield correlation(x.dropRight(i), y.drop(i),None)
  }

  def rollingCorr(x: Seq[Double], y:Seq[Double], maxWindow: Int): Seq[Double] = {
    for (i <- 0 to maxWindow-1) yield correlation(rollingAverage(x,i+1),y.drop(i),None)
  }

  /** 1D Least Squares
    *
    * @param x Input Values
    * @param y Output Values
    * @return Parameters (a,b) s.t. y ~ a*x+b
    */
  def linfit (x: Seq[Double], y:Seq[Double], weight: Option[Seq[Double]] = None): (Double,Double) = {
    val (xmean,xvar,ymean,yvar,cov) = if (weight==None) getStats(x,y) else getStatsWeighted(x,y,weight.get)
    (cov/xvar, ymean - xmean * cov/xvar )
  }

  /** R-squared metric
    *
    * @param prediction Prediction from fit
    * @param actual True values
    * @param weight Optional weighting parameter (ala weighted least squares)
    * @return
    */
  def rsquare (prediction: Seq[Double], actual: Seq[Double], weight: Option[Seq[Double]] = None): Double = {
    if (weight==None) {
      val (ymean, yvar) = getStats(actual)
      val sqerr = sqErr(prediction, actual)
      1.0 - sqerr / (prediction.length * yvar)
    } else {
      val (ymean, yvar) = getStatsWeighted(actual, weight.get)
      val sqerr = sqErr(prediction,actual,weight)
      1.0 - sqerr /(weight.get.sum * yvar)
    }
  }

  /* ===========================================================================================================  ====*/

  /** Square difference of two time series
    *
    * @param x
    * @param y
    * @return
    */
  def sqErr (x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]] = None): Double = {
    val dim = x.length
    require(y.length == dim, "Require: Input sequences must have same length")
    var err = 0.0
    if (weight==None) {
      x.zip(y).foreach{case (xv,yv) => err+=(yv-xv)*(yv-xv)}
    } else {
      val w = weight.get
      x.zip(y).zip(w).foreach {case ((xv,yv),wv) => err += wv*(yv-xv)*(yv-xv)}
    }
    err
  }

  def rmse (x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]] = None): Double = {
    if (weight==None) {
      val dim = x.length
      if (dim == 0) 0.0 else math.sqrt(sqErr(x, y) / dim)
    } else {
      var tot = weight.get.sum
      if (tot == 0) 0.0 else math.sqrt(sqErr(x,y,weight) / tot)
    }
  }

  /* ===================================== Derivatives! ======================================== */

  def derivCorrelation (x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]], order: Option[Int]): Double = {

    val filterOrder = order match {
      case Some(n) => n
      case None => 1
    }

    val xderiv = derivative.filteredDerivative(x, 1.0, filterOrder)
    val yderiv = derivative.filteredDerivative(y, 1.0, filterOrder)

    val dim = xderiv.length
    var score = 0.0
    var normx = 0.0
    var normy = 0.0

    if (weight == None) {
      for (i <- 0 to dim - 1) {
        score += xderiv(i) * yderiv(i)
        normx += xderiv(i) * xderiv(i)
        normy += yderiv(i) * yderiv(i)
      }
    } else {
      val w = weight.get
      for (i <- 0 to dim - 1) {
        score += w(i) * xderiv(i) * yderiv(i)
        normx += w(i) * xderiv(i) * xderiv(i)
        normy += w(i) * yderiv(i) * yderiv(i)
      }
    }
    if (score==0.0 || normx==0.0 || normy==0.0) 0.0 else score/math.sqrt(normx*normy)
  }

  /** Adjusted R-squared metric : for series having n date-points and p fit-parameters
    *
    * @param x
    * @param y
    * @param n
    * @param p
    * @return
    */
  def rsquareAdjusted (x: Seq[Double], y: Seq[Double], n: Int, p: Double, weight: Option[Seq[Double]] = None): Double = {
    val rsq = rsquare(x,y, weight)
    1.0 - (1.0-rsq)*(n-1.0)/(n-p-1.0)
  }

  /* Have realized that Sequences are terrible beasts - so we can either change to a better data type or rewrite this */
  def mape (x: Seq[Double], y: Seq[Double], weight: Option[Seq[Double]] = None, threshold: Double = 0.0001): Double = {

    //Define the threshold for which we drop (x,y) pair from MAPE calculation - as percent of max y-value.
    val yThresh = threshold*y.max
    require (x.length == y.length, "Require: Input sequences must have same length")

    var r = 0.0
    var tot = 0.0

    if (weight==None) {
      x.zip(y).foreach {case (xv,yv) =>
        if (yv >= yThresh) {
          r += math.abs((yv-xv)/yv)
          tot += 1.0
        }}
    } else {
      val w = weight.get
      x.zip(y).zip(w).foreach {case ((xv,yv),wv) =>
        if (yv >= yThresh && wv>0.0) {
          r += wv * math.abs((yv - xv) / yv)
          tot += wv
        }}
    }
    if (tot == 0) 0.0 else r / tot
  }

  /** Rescale series x by y (Option[Seq])
    *
    * @param x
    * @param y
    * @return
    */
  def normalizeTimeSeries (x: Seq[Double], y: Option[Seq[Double]]): Seq[Double] = {
    if (y == None) x
    else {
      require (y.get.length == x.length, "Require: Input sequences must have same length. numerator length:  " + x.length.toString + " scaling length:  " + y.get.length.toString)
      x.zip(y.get).map {case (n,d) => if (d==0) 0.0 else n/d}
    }
  }

  /** Filter method from R (not implementation - but what it outputs
    * Will output an array that is filter.length-1 shorter than the original,
    * since it uses no padding or other method to handle edge values
    */
  def Rfilter (x:Seq[Double], filter:Seq[Double]): Seq[Double] = {
    require(filter.length%2==1, "Odd filter length please")
    val F = filter.length
    val output  = scala.collection.mutable.ArrayBuffer[Double]()
    for (i <- F/2 to x.length-F/2-1) {
      var y = 0.0
      for (j <- 0 to F-1) {
        y += x(i-F/2+j)*filter(j)
      }
      output += y
    }
    output.toSeq
  }

  /** Decompose method from R **/
  def Rdecompose (x: Seq[Double], period: Int): (Seq[Double],Seq[Double],Seq[Double]) = {
    val f = if (period%2==1) Seq.fill(period)(1.0/period)
    else Seq(0.5/period)++Seq.fill(period-1)(1.0/period)++Seq(0.5/period)
    val trend = Rfilter(x,f)
    val F = f.length
    val season = x.slice(F/2, x.length-F/2).zip(trend).map{case (a,b)=> a-b}
    val periods = x.length/period
    val onePeriod = scala.collection.mutable.ArrayBuffer[Double]()
    for (i <- 0 to period-1) {
      var avg = 0.0
      var n = 0
      for (j <- 0 to periods+1) {
        if (j*period+i >= F/2 && j*period+i < x.length-F/2) {
          avg += season(period * j + i - F / 2)
          n += 1
        }
      }
      onePeriod += avg/n
    }
    val onePeriodAvg = onePeriod.sum/onePeriod.length
    for (i <- 0 to period-1) onePeriod(i) -= onePeriodAvg
    var seasonal = scala.collection.mutable.ArrayBuffer[Double]()
    for (j <- 0 to periods) seasonal ++= onePeriod
    seasonal = seasonal.slice(0, x.length)
    val residual = x.zip(seasonal).map{case (x,y) => x-y}
    (x, seasonal, residual)
  }

  def medianFilter(x: Seq[Double], window: Int): Seq[Double] = {
    val padded = Seq.fill(window-1){x(0)}++x++Seq.fill(window-1){x.last}
    padded.sliding(window).map{(_.sortWith(_<_)(window/2))}.toSeq
  }

  def rollingAverage(x: Seq[Double], window: Int): Seq[Double] = {
    val xa = x.toArray
    var v = xa.slice(0,window).sum/window
    val out = scala.collection.mutable.ArrayBuffer(v)
    for (i <- window to x.length-1) {
      v += (xa(i)-xa(i-window))/window
      out += v
    }
    out.toSeq
  }

  def rollingAverageNoLoss(x: Seq[Double], window: Int): Seq[Double] = {
    val xa = x.toArray
    val out = scala.collection.mutable.ArrayBuffer[Double]()
    for (i <- 1 to window-1) {
      out += xa.slice(0,i).sum/i.toDouble
    }
    var v = xa.slice(0,window).sum/window
    out += v
    for (i <- window to x.length-1) {
      v += (xa(i)-xa(i-window))/window
      out += v
    }
    out.toSeq
  }


}