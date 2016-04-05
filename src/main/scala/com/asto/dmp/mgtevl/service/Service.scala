package com.asto.dmp.mgtevl.service

import com.asto.dmp.mgtevl.base.{Constants, Contexts}
import org.apache.spark.Logging

trait Service extends Logging with scala.Serializable {
  protected val sqlContext = Contexts.sqlContext
  protected var errorInfo: String = s"${getClass.getSimpleName}的run()方法出现异常"

  protected def handlingExceptions(t: Throwable) {

    logError(errorInfo, t)
    Constants.App.ERROR_LOG.append(s"$errorInfo\n${t.toString}\n${t.getStackTraceString}\n")
  }

  protected def printStartLog() = logInfo(s"开始运行${getClass.getSimpleName}的run()方法")

  protected def printEndLog() = logInfo(s"${getClass.getSimpleName}的run()方法运行结束")

  private[service] def runServices()

  def run() {
    try {
      printStartLog()
      runServices()
    } catch {
      case t: Throwable => handlingExceptions(t)
    } finally {
      printEndLog()
    }
  }
}
