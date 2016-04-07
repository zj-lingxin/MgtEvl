package com.asto.dmp.mgtevl.service.impl

import com.asto.dmp.mgtevl.base.{Paths, Constants, JdbcDF}
import com.asto.dmp.mgtevl.service.Service
import com.asto.dmp.mgtevl.util.{FileUtils, Utils, DateUtils}
import org.apache.spark.rdd.RDD

class MainService extends Service {

  private val metricDF = {
    var sql = "select x.property_uuid, x.target_time, x.metric_dim_code, x.metric_value from xdgc.property_metric x where metric_dim_code in ('M_SALE_AMOUNT','M_DEAL_CUST_CNT','M_CUST_ONE_PRICE','M_REFUND_RATE','M_TOP3_GOODS_REVENUE','M_TOP10_CUST_REVENUE') "
    if (Constants.IS_ONLINE) {
      sql += s" and x.property_uuid = '${Constants.PROPERTY_UUID}'"
    }
    JdbcDF.load(s"($sql) tmp")
  }
  //统计近n个月的数据
  private val n = 6
  private val (startDate, endDate) = (DateUtils.monthsAgo(n, "yyyyMM"), DateUtils.monthsAgo(1, "yyyyMM"))
  private val (startDateBackOneYear, endDateBackOneYear) = (DateUtils.monthsAgo(n + 12, "yyyyMM"), DateUtils.monthsAgo(13, "yyyyMM"))

  private def getLastNMonthInfoBy[T](metricCode: String, f: String => T, startTime: String = startDate, endTime: String = endDate): RDD[(String, String, T)] = {
    metricDF.select("property_uuid", "target_time", "metric_value", "metric_dim_code")
      .filter(s"metric_dim_code = '$metricCode' and target_time >= '$startTime' and target_time <= '$endTime'")
      .map(a => (a(0).toString, a(1).toString, f(a(2).toString))).distinct()
  }

  //近n个月的交易额
  private val saleAmount = getLastNMonthInfoBy("M_SALE_AMOUNT", v => v.toDouble).persist()

  private val saleAmountBackOneYear = getLastNMonthInfoBy("M_SALE_AMOUNT", v => v.toDouble, startDateBackOneYear, endDateBackOneYear)

  private val saleAmountTotal = saleAmount.map(t => (t._1, t._3)).reduceByKey(_ + _).persist()

  //近n个月的交易笔数(包含非有效交易笔数)
  private val dealCustCnt = getLastNMonthInfoBy("M_DEAL_CUST_CNT", v => v.toInt)

  //近n个月的客单价
  private val custOnePrice = getLastNMonthInfoBy("M_CUST_ONE_PRICE", v => v.toDouble)

  //近n个月的退款率
  private val refundRate = getLastNMonthInfoBy("M_REFUND_RATE", v => v.replaceAll("%", "").toDouble)

  //近n个月的top3的商品销售额
  private val top3GoodsRevenue = getLastNMonthInfoBy("M_TOP3_GOODS_REVENUE", v => v.toDouble)

  //近n个月的top10客户销售额
  private val top10CustRevenue = getLastNMonthInfoBy("M_TOP10_CUST_REVENUE", v => v.toDouble)

  //每个月的销售额增长倍数
  private val growthMultiples = getGrowthMultiplesEveryMonths

  private def getGrowthMultiplesEveryMonths = {
    val addOneMonth = (1 to n).map { i => (DateUtils.monthsAgo(i, "yyyyMM"), DateUtils.monthsAgo(i - 1, "yyyyMM")) }.toMap
    val ai1 = saleAmount.map(t => ((t._1, addOneMonth(t._2)), t._3))
    val ai = saleAmount.map(t => ((t._1, t._2), t._3))
    ai.leftOuterJoin(ai1).filter(_._2._2.isDefined)
      .map(t => (t._1._1, t._2._1 / t._2._2.get))
  }

  /**
   * 近n个月月均交易额
   */
  def getAvgSaleAmount = saleAmountTotal.mapValues(_ / n)

  /**
   * 近n个月月均交易笔数
   */
  def getDealCustCnt = dealCustCnt.map(t => (t._1, t._3.toInt)).reduceByKey(_ + _).mapValues(_ / n)

  /**
   * 近n个月退款率
   * 设ri为第i个月的退款率，ai为第i个月的交易额，则近n个月月均退款率为 sum(ri * ai) / sum(ai) * 100%
   */
  def getRefundRate = {
    val s = saleAmount.map(t => ((t._1, t._2), t._3))
    //求出sum(ri * ai)
    val refundAmount = refundRate.map(t => ((t._1, t._2), t._3)).leftOuterJoin(s)
      .map(t => (t._1._1, t._2._1.toDouble * 0.01 * t._2._2.getOrElse(0D)))
      .reduceByKey(_ + _)

    //求出sum(ri * ai) / sum(ai) * 100%
    saleAmountTotal.leftOuterJoin(refundAmount)
      .filter(t => t._2._1 != 0D && t._2._2.isDefined)
      .mapValues(t => t._2.get / t._1 * 100)
  }

  /**
   * 求变异系数(CoefficientOfVariation,简写为CV) CV = S(样本标准偏差) / M(平均值)
   * 样本标准偏差 S = sqrt( sum( (Xi - M)`^`2 ) / (N-1) )   其中N是样本个数
   */
  private def getCV(data: RDD[(String, Double)]) = {
    // N-1
    val nMinusOne = data.mapValues(_ => 1).foldByKey(-1)(_ + _)

    //M 平均值
    val mean = data.mapValues((_, 1)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(v => v._1 / v._2)

    data.leftOuterJoin(mean)
      // (Xi - M)^2
      .mapValues(v => Math.pow(v._1 - v._2.getOrElse(0D), 2))
      // sum( (Xi - M)^2 )
      .reduceByKey(_ + _)
      // (913756e3254342a28cd6ec7af0eb8add,(2.6700562858777084E10,Some(5)))
      .leftOuterJoin(nMinusOne)
      //除数不能等于0
      .filter(tuple => tuple._2._2 != Some(0))
      // sqrt( sum( (Xi - M)^2 ) / (N-1) )
      .mapValues(v => Math.sqrt(v._1 / v._2.getOrElse(n)))
      // (913756e3254342a28cd6ec7af0eb8add,(73076.07386659068,Some(133248.53166666668)))
      .leftOuterJoin(mean)
      .filter(_._2._2.isDefined)
      .mapValues(v => v._1 / v._2.get * 100) //最后的值就是“变异系数” 百分数，所以*100
  }

  /**
   * 近n个月交易额变异系数
   */
  def getSaleAmountCV = {
    val saleAmountPair = saleAmount.map(t => (t._1, t._3)).persist()
    getCV(saleAmountPair)
  }

  /**
   * 近n个月客单价变异系数
   */
  def getCustOnePriceCV = {
    val custOnePricePair = custOnePrice.map(t => (t._1, t._3)).persist()
    getCV(custOnePricePair)
  }

  /**
   * 近n个月交易金额前三商品的金额占比
   */
  def getTop3GoodsRevenueRate = {
    val top3GoodsRevenuePair = top3GoodsRevenue.map(t => (t._1, t._3))
    getTopAmountRate(top3GoodsRevenuePair)
  }

  /**
   * 近n个月金额前十客户的金额占比
   */
  def getgrowthRate = {
    val top10CustRevenuePair = top10CustRevenue.map(t => (t._1, t._3))
    getTopAmountRate(top10CustRevenuePair)
  }

  /**
   * 求top前几的金额占比
   */
  def getTopAmountRate(data: RDD[(String, Double)]) = {
    data.reduceByKey(_ + _)
      .leftOuterJoin(saleAmountTotal)
      .filter(_._2._2.isDefined)
      .mapValues(v => v._1 / v._2.get * 100) //百分数*100
  }

  /**
   * 同期交易增长率(与去年同月相比)
   */
  def getGrowthRate = {
    val getAvgSaleAmount = (data: RDD[(String, String, Double)]) => data.map(t => (t._1, (t._3, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(v => v._1 / v._2)

    val avgMonthSalesBackOneYear = getAvgSaleAmount(saleAmountBackOneYear)
    val avgMonthSales = getAvgSaleAmount(saleAmount)

    avgMonthSales.leftOuterJoin(avgMonthSalesBackOneYear).filter(_._2._2.isDefined)
      .mapValues(v => v._1 / v._2.get * 100) //百分数 * 100
  }

  /**
   * 近n个月月均交易增长倍数
   * sum(ai/a(i+1)) / n-1   i取值：i = 1,..,n-1
   */
  def getSalesAmountGrowthMultiples = {
    growthMultiples.mapValues((_, 1))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(v => v._1 / v._2 * 100) //百分数 * 100
  }


  /**
   * 近n个月交易上涨的月份数（环比上涨10%才算上涨）
   */
  def getSaleAmountIncreaseMonthNum = {
    growthMultiples.mapValues(v => if (v >= 1.1) 1 else 0).reduceByKey(_ + _)
  }

  /**
   * 近n个月月均交易额评分
   */
  val saleAmountScore = (saleAmount: Double) => {
    if (saleAmount < 5000) 1
    else if (saleAmount >= 5000 && saleAmount < 10000) 3
    else if (saleAmount >= 10000 && saleAmount < 15000) 5
    else if (saleAmount >= 15000 && saleAmount < 20000) 6
    else if (saleAmount >= 20000 && saleAmount < 25000) 8
    else if (saleAmount >= 25000) 10
  }

  /**
   * 近n个月月均交易笔数评分
   */
  val dealCustCntScore = (dealCustCnt: Int) => {
    if (dealCustCnt < 750) 6
    else if (dealCustCnt >= 750 && dealCustCnt < 1500) 8
    else if (dealCustCnt >= 1500 && dealCustCnt < 2500) 10
    else if (dealCustCnt >= 2500 && dealCustCnt < 3000) 1
    else if (dealCustCnt >= 3000 && dealCustCnt < 4000) 3
    else if (dealCustCnt >= 4000) 5
  }

  /**
   * 近n个月退款率评分
   */
  val refundRateScore = (refundRate: Double) => {
    if (refundRate < 5) 10
    else if (refundRate >= 5 && refundRate < 7) 8
    else if (refundRate >= 7 && refundRate < 10) 6
    else if (refundRate >= 10 && refundRate < 20) 5
    else if (refundRate >= 20 && refundRate < 35) 3
    else if (refundRate >= 35) 1
  }

  /**
   * 近n个月交易额变异系数评分
   */
  val saleAmountCVScore = (saleAmountCVScore: Double) => {
    if (saleAmountCVScore < 40) 10
    else if (saleAmountCVScore >= 40 && saleAmountCVScore < 60) 7
    else if (saleAmountCVScore >= 60 && saleAmountCVScore < 90) 4
    else if (saleAmountCVScore >= 90) 1
  }

  /**
   * 近n个月客单价变异系数评分
   */
  val custOnePriceCVScore = (custOnePriceCV: Double) => {
    if (custOnePriceCV < 20) 10
    else if (custOnePriceCV >= 20 && custOnePriceCV < 40) 7
    else if (custOnePriceCV >= 40 && custOnePriceCV < 70) 4
    else if (custOnePriceCV >= 70) 1
  }

  /**
   * 交易金额前三商品的金额占比评分
   */
  val top3GoodsRevenueRateScore = (top3GoodsRevenueRate: Double) => {
    if (top3GoodsRevenueRate < 20) 10
    else if (top3GoodsRevenueRate >= 20 && top3GoodsRevenueRate < 40) 7
    else if (top3GoodsRevenueRate >= 40 && top3GoodsRevenueRate < 70) 4
    else if (top3GoodsRevenueRate >= 70) 1
  }

  /**
   * 金额前十客户的金额占比评分
   */
  def top10CustRevenueRateScore = (top10CustRevenueRate: Double) => {
    if (top10CustRevenueRate < 20) 10
    else if (top10CustRevenueRate >= 20 && top10CustRevenueRate < 50) 7
    else if (top10CustRevenueRate >= 50 && top10CustRevenueRate < 90) 4
    else if (top10CustRevenueRate >= 90) 1
  }

  /**
   * 交易增长倍数评分
   */
  val growthMultiplesScore = (growthMultiples: Double) => {
    if (growthMultiples < 70) 1
    else if (growthMultiples >= 70 && growthMultiples < 160) 4
    else if (growthMultiples >= 160 && growthMultiples < 230) 7
    else if (growthMultiples >= 230) 10
  }

  /**
   * 同期交易增长率评分
   */
  val growthRateScore = (growthRate: Double) => {
    if (growthRate < 50) 2
    else if (growthRate >= 50 && growthRate < 80) 4
    else if (growthRate >= 80 && growthRate < 120) 6
    else if (growthRate >= 120 && growthRate < 150) 8
    else if (growthRate >= 150) 10
  }

  /**
   * 近n个月交易上涨的月份数（环比上涨10%才算上涨）
   */
  val increaseMonthNumScore = (increaseMonthNum: Int) => {
    increaseMonthNum match {
      case 0 => 0
      case 1 => 2
      case 2 => 4
      case 3 => 6
      case 4 => 8
      case _ => 10
    }
  }

  def getQuotaScores(quotas: RDD[(String, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]) = {
    def retainNone[T <: Any](v: Any, f: T => Any) = {
      v match {
        case _: Double => f(v.toString.toDouble.asInstanceOf[T])
        case _: Int => f(v.toString.toInt.asInstanceOf[T])
        case _ => None
      }
    }
    quotas.map(t => (t._1,
      retainNone(t._2, saleAmountScore),
      retainNone(t._3, dealCustCntScore),
      retainNone(t._4, refundRateScore),
      retainNone(t._5, saleAmountCVScore),
      retainNone(t._6, custOnePriceCVScore),
      retainNone(t._7, top3GoodsRevenueRateScore),
      retainNone(t._8, top10CustRevenueRateScore),
      retainNone(t._9, growthMultiplesScore),
      retainNone(t._10, growthRateScore),
      retainNone(t._11, increaseMonthNumScore)
      ))
  }

  /**
   * 计算最终得分。以下是各个指标的权重：
   * 近n个月月均交易额	25%
   * 近n个月月均有效交易笔数	10%
   * 近n个月退款率	10%
   * 近n个月交易额变异系数	5.0%
   * 近n个月客单价变异系数	10%
   * 近n个月交易金额前三商品的金额占比	10%
   * 近n个月交易金额前十客户的金额占比	10%
   * 同期交易增长率	5.0%
   * 近n个月交易增长倍数	7.5%
   * 近n个月交易上涨的月份数（环比上涨10%）	7.5%
   */
  def getFinalScore(quotaScores: RDD[(String, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]) = {
    //将None的得分改为0
    def scoreWeightHandle[T <: Any](v: T, weight: Double): (Double, Double) = {
      v match {
        case _: Int => (v.toString.toInt * weight, weight)
        case _ => (0D, 0D)
      }
    }
    def getFinalScore(scoresWeight: Array[(Any, Double)]) = {
      val totalScoreWeight = scoresWeight.map(s => scoreWeightHandle(s._1, s._2)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      Math.round(totalScoreWeight._1 / totalScoreWeight._2 * 10)
    }
    quotaScores.map(t => (t._1, getFinalScore(Array((t._2, 0.25), (t._3, 0.1), (t._4, 0.1), (t._5, 0.05), (t._6, 0.1), (t._7, 0.1), (t._8, 0.1), (t._9, 0.05), (t._10, 0.075), (t._11, 0.075)))))
  }

  def getAllQuotas = {
    import com.asto.dmp.mgtevl.util.RichPairRDD._
    getAvgSaleAmount.cogroup(getDealCustCnt, getRefundRate, getSaleAmountCV, getCustOnePriceCV, getTop3GoodsRevenueRate, getgrowthRate, getGrowthRate, getSalesAmountGrowthMultiples, getSaleAmountIncreaseMonthNum)
      .map(t => (t._1, Utils.firstItrAsDouble(t._2._1), Utils.firstItrAsInt(t._2._2), Utils.firstItrAsDouble(t._2._3),
      Utils.firstItrAsDouble(t._2._4), Utils.firstItrAsDouble(t._2._5), Utils.firstItrAsDouble(t._2._6), Utils.firstItrAsDouble(t._2._7),
      Utils.firstItrAsDouble(t._2._8), Utils.firstItrAsDouble(t._2._9), Utils.firstItrAsInt(t._2._10)))
  }

  override private[service] def runServices(): Unit = {
    val path = new Paths()

    val quotas = getAllQuotas
    val quotaScores = getQuotaScores(quotas)
    val finalScores = getFinalScore(quotaScores)

    FileUtils.saveFileToHDFS(path.quotasPath, quotas)
    FileUtils.saveFileToHDFS(path.quotaScoresPath, quotaScores)
    FileUtils.saveFileToHDFS(path.finalScoresPath, finalScores)
  }
}

