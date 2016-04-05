package com.asto.dmp.mgtevl.base

import com.asto.dmp.mgtevl.service.impl.MainService
import org.apache.spark.Logging


object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    new MainService().run()

    closeResources()
    printEndLogs(startTime)
  }


  /**
   * 关闭用到的资源
   */
  private def closeResources() = {
   // MQAgent.close()
    Contexts.stopSparkContext()
  }


  /**
   * 打印程序运行的时间
   */
  private def printRunningTime(startTime: Long) {
    logInfo((s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  /**
   * 如果程序在运行过程中出现错误。那么在程序的最后打印出这些错误。
   * 之所以这么做是因为，Spark的Info日志太多，往往会把错误的日志淹没。
   */
  private def printErrorLogsIfExist() {
    if (Constants.App.ERROR_LOG.toString != "") {
      logError((s"程序在运行过程中遇到了如下错误：${Constants.App.ERROR_LOG.toString}"))
    }
  }

  /**
   * 最后打印出一些提示日志
   */
  private def printEndLogs(startTime: Long): Unit = {
    printErrorLogsIfExist()
    printRunningTime(startTime: Long)
  }

}