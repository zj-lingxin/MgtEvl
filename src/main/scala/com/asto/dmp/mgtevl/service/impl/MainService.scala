package com.asto.dmp.mgtevl.service.impl

import com.asto.dmp.mgtevl.service.impl.MainService._
import com.asto.dmp.mgtevl.base.JdbcDF
import com.asto.dmp.mgtevl.service.Service
import com.asto.dmp.mgtevl.util.DateUtils

class MainService extends Service {
  //近n个月月均交易额
  def getAvgSaleAmount = saleAmount.map(t => (t._1, t._3.toDouble)).reduceByKey(_ + _).mapValues(_ / n)

  //近n个月月均交易笔数
  def getDealCustCnt = dealCustCnt.map(t => (t._1, t._3.toInt)).reduceByKey(_ + _).mapValues(_ / n)

  override private[service] def runServices(): Unit = {
    //getAvgSaleAmount.foreach(println)
    getDealCustCnt.foreach(println)
  }
}

object MainService {
  val metricDF = JdbcDF.load("xdgc.property_metric")
  //统计近n个月的数据
  val n = 6
  val (startDate, endDate) = (DateUtils.monthsAgo(n, "yyyyMM"), DateUtils.monthsAgo(1, "yyyyMM"))

  private def getLastNMonthInfoBy(metricCode: String) = {
    metricDF.select("property_uuid", "target_time", "metric_value", "metric_dim_code")
      .filter(s"metric_dim_code = '$metricCode' and target_time >= '$startDate' and target_time <= '$endDate'")
      .map(a => (a(0).toString, a(1).toString, a(2).toString))
  }

  //近n个月的交易额
  val saleAmount = getLastNMonthInfoBy("M_SALE_AMOUNT")

  //近n个月的交易笔数(包含非有效交易笔数)
  val dealCustCnt = getLastNMonthInfoBy("M_DEAL_CUST_CNT")

  //近n个月的客单价
  val custOnePrice = getLastNMonthInfoBy("M_CUST_ONE_PRICE")

  //近n个月的退款率
  val refundRate = getLastNMonthInfoBy("M_REFUND_RATE")

  //近n个月的top3的商品销售额
  val top3GoodsRevenue = getLastNMonthInfoBy("M_TOP3_GOODS_REVENUE")

  //近n个月的top10客户销售额
  val top10CustRevenue = getLastNMonthInfoBy("M_TOP10_CUST_REVENUE")

}




