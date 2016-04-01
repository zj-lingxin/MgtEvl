package com.asto.dmp.mgtevl.util

import com.asto.dmp.mgtevl.base.Constants
import com.asto.dmp.ycd.base.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * 文件相关的工具类
 */
object FileUtils extends Logging {
  private val conf = new Configuration()
  conf.set("fs.defaultFS", Constants.Hadoop.DEFAULT_FS)
  conf.set("mapreduce.jobtracker.address", Constants.Hadoop.JOBTRACKER_ADDRESS)

  def deleteFilesInHDFS(paths: String*) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val HDFSFilesSystem = filePath.getFileSystem(new Configuration())
      if (HDFSFilesSystem.exists(filePath)) {
        logInfo(s"删除目录：$filePath")
        HDFSFilesSystem.delete(filePath, true)
      }
    }
  }

  def saveAsTextFile[T <: Product](rdd: RDD[T], savePath: String) = {
    deleteFilesInHDFS(savePath)
    logInfo(s"往${savePath}中写入信息")
    rdd.map(_.productIterator.mkString(Constants.OutputPath.SEPARATOR)).coalesce(1).saveAsTextFile(savePath)
  }

  def saveAsTextFile(text: String, savePath: String) = {
    deleteFilesInHDFS(savePath)
    logInfo(s"往${savePath}中写入信息")
    val out = FileSystem.get(conf).create(new Path(savePath))
    out.write(text.getBytes)
    out.flush()
    out.close()
  }
}
