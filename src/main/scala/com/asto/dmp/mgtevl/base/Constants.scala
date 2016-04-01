package com.asto.dmp.mgtevl.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val NAME = "经营管理"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/mgtevl"
    var TODAY: String = _
    var STORE_ID: String = _
    var IS_ONLINE: Boolean = true
    var RUN_CODE: String = _
    var TIMESTAMP: Long = _
    val ERROR_LOG: StringBuffer = new StringBuffer("")
    var MESSAGES: StringBuffer = new StringBuffer("")
  }
  
  object Hadoop {
    val JOBTRACKER_ADDRESS = "appcluster"
    val DEFAULT_FS = s"hdfs://$JOBTRACKER_ADDRESS"
  }

  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"

    //在线目录
    private val ONLINE_DIR = s"${App.DIR}/input/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    //离线目录
    private val OFFLINE_DIR = s"${App.DIR}/input/offline/${App.TODAY}"

    private val file = s"$ONLINE_DIR/fileName/*"
  }
  

  /** 输出文件路径 **/
  object OutputPath {
    private val dirAndFileName = (fileName: String) => if (App.IS_ONLINE) s"$ONLINE_DIR/$fileName" else s"$OFFLINE_DIR/$fileName"
    val SEPARATOR = "\t"
    private val ONLINE_DIR = s"${App.DIR}/output/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    private val OFFLINE_DIR = s"${App.DIR}/output/offline/${App.TODAY}/${App.TIMESTAMP}"

    val LOAN_WARN_PATH = s"$OFFLINE_DIR/loanWarn"
  }

  /** 表的模式 **/
  object Schema {
    //烟草订单详情：店铺id,订单号,订货日期,卷烟名称,批发价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额,生产厂家,地区编码
    val ORDER_DETAILS = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,producer_name,area_code"
  }
}
