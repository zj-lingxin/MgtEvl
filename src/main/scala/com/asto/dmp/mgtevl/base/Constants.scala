package com.asto.dmp.mgtevl.base

import com.asto.dmp.mgtevl.util.DateUtils

object Constants {
  val APP_NAME = "经营管理"
  val DIR = s"${Props.get("fs.defaultFS")}/mgtevl"
  var PROPERTY_UUID: String = _
  val OUTPUT_SEPARATOR = "\t"
  var IS_ONLINE = true
  val TODAY = DateUtils.getStrDate("yyyyMM/dd")
}

class Paths {
  import com.asto.dmp.mgtevl.base.Constants._
  private val onlineDir = s"$DIR/online/$TODAY/$PROPERTY_UUID"
  private val offlineDir = s"$DIR/offline/$TODAY"
  private val dirAndFile = (fileName: String) => {
    if (IS_ONLINE) s"$onlineDir/$fileName" else s"$offlineDir/$fileName"
  }
  val quotasPath = dirAndFile("quotas")
  val quotaScoresPath = dirAndFile("quotaScores")
  val finalScoresPath = dirAndFile("finalScores")
}