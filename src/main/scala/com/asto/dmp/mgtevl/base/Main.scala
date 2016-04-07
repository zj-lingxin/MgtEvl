package com.asto.dmp.mgtevl.base

import com.asto.dmp.mgtevl.service.impl.MainService
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    checkArgs(args)
    new MainService().run()
    closeResources()

    logInfo((s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  def checkArgs(args: Array[String]) = {
    logInfo("单个实时运行时需要传入“property_uuid”, 全量运行时不需要参数。")
    if (args.length == 1) {
      Constants.PROPERTY_UUID = args(0)
      Constants.IS_ONLINE = true
      logInfo(s"单个实时运行。 property_uuid:${Constants.PROPERTY_UUID }")
    } else {
      Constants.IS_ONLINE = false
      logInfo("全量运行")
    }
  }
  /**
   * 关闭用到的资源
   */
  private def closeResources() = {
   // MQAgent.close()
    Contexts.stopSparkContext()
  }
}