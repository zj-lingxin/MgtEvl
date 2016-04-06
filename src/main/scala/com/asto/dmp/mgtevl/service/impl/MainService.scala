package com.asto.dmp.mgtevl.service.impl

import com.asto.dmp.mgtevl.service.impl.MainService._
import com.asto.dmp.mgtevl.base.JdbcDF
import com.asto.dmp.mgtevl.service.Service
import com.asto.dmp.mgtevl.util.DateUtils
import org.apache.spark.rdd.RDD

class MainService extends Service {
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
      // sqrt( sum( (Xi - M)^2 ) / (N-1) )
      .mapValues(v => Math.sqrt(v._1 / v._2.getOrElse(n)))
      // (913756e3254342a28cd6ec7af0eb8add,(73076.07386659068,Some(133248.53166666668)))
      .leftOuterJoin(mean)
      .filter(_._2._2.isDefined)
      .mapValues(v => v._1 / v._2.get) //最后的值就是“变异系数”
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
  def getTop10CustRevenueRate = {
    val top10CustRevenuePair = top10CustRevenue.map(t => (t._1, t._3))
    getTopAmountRate(top10CustRevenuePair)
  }

  /**
   * 求top前几的金额占比
   * @param data
   */
  def getTopAmountRate(data: RDD[(String, Double)]) = {
    data.reduceByKey(_ + _)
      .leftOuterJoin(saleAmountTotal)
      .filter(_._2._2.isDefined)
      .mapValues(v => v._1 / v._2.get)
  }

  override private[service] def runServices(): Unit = {
    getTop3GoodsRevenueRate.foreach(println)
    println("!!!!!!!")
    getTop10CustRevenueRate.foreach(println)
  }
}

object MainService {
  val metricDF = JdbcDF.load("xdgc.property_metric")
  //统计近n个月的数据
  val n = 6
  val (startDate, endDate) = (DateUtils.monthsAgo(n, "yyyyMM"), DateUtils.monthsAgo(1, "yyyyMM"))

  private def getLastNMonthInfoBy[T](metricCode: String, f: String => T): RDD[(String, String, T)] = {
    metricDF.select("property_uuid", "target_time", "metric_value", "metric_dim_code")
      .filter(s"metric_dim_code = '$metricCode' and target_time >= '$startDate' and target_time <= '$endDate'")
      .map(a => (a(0).toString, a(1).toString, f(a(2).toString)))
  }

  //近n个月的交易额
  val saleAmount = getLastNMonthInfoBy("M_SALE_AMOUNT", v => v.toDouble)

  val saleAmountTotal = saleAmount.map(t => (t._1, t._3)).reduceByKey(_ + _).persist()

  //近n个月的交易笔数(包含非有效交易笔数)
  val dealCustCnt = getLastNMonthInfoBy("M_DEAL_CUST_CNT", v => v.toInt)

  //近n个月的客单价
  val custOnePrice = getLastNMonthInfoBy("M_CUST_ONE_PRICE", v => v.toDouble)

  //近n个月的退款率
  val refundRate = getLastNMonthInfoBy("M_REFUND_RATE", v => v.replaceAll("%", "").toDouble)

  //近n个月的top3的商品销售额
  val top3GoodsRevenue = getLastNMonthInfoBy("M_TOP3_GOODS_REVENUE", v => v.toDouble)

  //近n个月的top10客户销售额
  val top10CustRevenue = getLastNMonthInfoBy("M_TOP10_CUST_REVENUE", v => v.toDouble)

}




