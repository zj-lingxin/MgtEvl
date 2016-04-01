package com.asto.dmp.mgtevl.util

import java.util.Calendar

object BizUtils {
  /**
   * 输入一个时间，计算这个时间距离当前的时间有多少个月份
   */
  def monthsNumFrom(date: String, format: String) = {
    val now = Calendar.getInstance()
    val theEarliestDate = DateUtils.strToCalendar(date, format)
    (now.get(Calendar.YEAR) * 12 + now.get(Calendar.MONTH)) -
      (theEarliestDate.get(Calendar.YEAR) * 12 + theEarliestDate.get(Calendar.MONTH))
  }
}
