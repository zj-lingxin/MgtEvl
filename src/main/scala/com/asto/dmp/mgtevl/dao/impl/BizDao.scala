package com.asto.dmp.mgtevl.dao.impl


object BizDao {

}

object Helper {
  implicit val storeIdAndOrderDateOrdering: Ordering[(String, String)] = new Ordering[(String, String)] {
    override def compare(a: (String, String), b: (String, String)): Int =
      if (a._1 > b._1) 1
      else if (a._1 < b._1) -1
      else if (a._2 > b._2) -1
      else 1
  }
}