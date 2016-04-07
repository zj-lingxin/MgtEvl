package com.asto.dmp.mgtevl.service

import com.asto.dmp.mgtevl.base.{Constants, Contexts}
import org.apache.spark.Logging

trait Service extends Logging with scala.Serializable {
  protected val sqlContext = Contexts.sqlContext
  protected var errorInfo: String = s"${getClass.getSimpleName}的run()方法出现异常"

  private[service] def runServices()

  def run() {
    try {
      logInfo(s"开始运行${getClass.getSimpleName}的run()方法")
      runServices()
    } catch {
      case t: Throwable => logError(errorInfo, t)
    } finally {
      logInfo(s"${getClass.getSimpleName}的run()方法运行结束")
    }
  }
}
