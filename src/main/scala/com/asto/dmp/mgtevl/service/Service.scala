package com.asto.dmp.mgtevl.service

import com.asto.dmp.ycd.base.{Constants, Contexts}
import com.asto.dmp.ycd.util.Utils
import com.asto.dmp.ycd.util.mail.MailAgent
import org.apache.spark.Logging

trait Service extends Logging {
  protected val sqlContext = Contexts.sqlContext
  protected var mailSubject: String = s"${getClass.getSimpleName}的run()方法出现异常"

  protected def handlingExceptions(t: Throwable) {
    MailAgent(t, mailSubject).sendMessage()
    logError(mailSubject, t)
    Constants.App.ERROR_LOG.append(s"$mailSubject\n${t.toString}\n${t.getStackTraceString}\n")
  }

  protected def printStartLog() = logInfo(Utils.logWrapper(s"开始运行${getClass.getSimpleName}的run()方法"))

  protected def printEndLog() = logInfo(Utils.logWrapper(s"${getClass.getSimpleName}的run()方法运行结束"))

  protected def runServices()

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
