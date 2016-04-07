package com.asto.dmp.mgtevl.util

import java.io.{InputStreamReader, BufferedReader}
import com.asto.dmp.mgtevl.base.Props
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * 文件相关的工具类
 */
object FileUtils extends Logging {
  private val conf = new Configuration()
  conf.set("fs.defaultFS", Props.get("fs.defaultFS"))
  conf.set("mapreduce.jobtracker.address", Props.get("mapreduce.jobtracker.address"))

  private val fileSystem = FileSystem.get(conf) //hdfs文件系统

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

  def mkdirs(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
  /**
   * 将RDD保存到HDFS
   * @param path
   */
  def saveFileToHDFSForStr(path: String, rdd: RDD[String]) = {
    deleteFilesInHDFS(path)
    logInfo(s"往${path}中写入信息")
    rdd.coalesce(1).saveAsTextFile(path)
  }

  /**
   * 将RDD保存到HDFS
   * @param separator RDD元素之间的分隔符
   */
  def saveFileToHDFS[T <: Product](path: String, rdd: RDD[T],  separator: String = "\t") = {
    deleteFilesInHDFS(path)
    logInfo(s"往${path}中写入信息")
    rdd.map(_.productIterator.mkString(separator)).coalesce(1).saveAsTextFile(path)
  }

  /**
   * 将文本文件保存到HDFS
   */
  def saveFileToHDFS(path: String, text: String) = {
    deleteFilesInHDFS(path)
    logInfo(s"往${path}中写入信息")
    val out = fileSystem.create(new Path(path))
    out.write(text.getBytes)
    out.flush()
    out.close()
  }


  /**
   *保存文件到本地
   */
  def saveFileToLocal(path: String, text: String) = {
    scala.tools.nsc.io.File(path).writeAll(text)
  }

  /**
   *删除本地文件和目录
   */
  def deleteFileInLocal(path: String) = {
    scala.tools.nsc.io.File(path).deleteRecursively()
  }

  /**
   * 从HDFS读取文件
   * @param path
   * @return
   */
  def readContextFromHDFS(path: String) = {
    readLinesFromHDFS(path).mkString("\n")
  }

  def readLinesFromHDFS(path: String): Array[String] = {
    val pt = new Path(path)
    val br: BufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(pt)))
    val lines = scala.collection.mutable.ArrayBuffer[String]()
    var line = br.readLine()
    while (line != null) {
      lines += line
      line = br.readLine()
    }
    lines.toArray
  }
}
