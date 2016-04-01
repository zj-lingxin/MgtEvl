package com.asto.dmp.mgtevl.dao

import com.asto.dmp.mgtevl.base.{Constants, Contexts}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

trait Dao extends Logging {
  private val sqlContext = Contexts.sqlContext

  protected def getProps(inputFilePath: String, schema: String, tempTableName: String, sqlObj: SQL, separator: String = Constants.InputPath.SEPARATOR) = {
    registerTempTableIfNotExist(inputFilePath, schema, tempTableName, separator)
    combineSqlAndQuery(tempTableName, sqlObj)
  }

  protected def getProps[A <: Product : TypeTag](rdd: RDD[A], schema: String, tempTableName: String, sqlObj: SQL) = {
    registerTempTableIfNotExist(rdd, schema, tempTableName)
    combineSqlAndQuery(tempTableName, sqlObj)
  }

  private def combineSqlAndQuery(tempTableName: String, sqlObj: SQL) = {
    var _sql = s"SELECT ${sqlObj.select} FROM $tempTableName"

    if (Option(sqlObj.where).isDefined)
      _sql += s" WHERE ${sqlObj.where}"

    if (Option(sqlObj.orderBy).isDefined) {
      //使用OrderBy的时候，需要将spark.sql.shuffle.partitions设小
      sqlContext.sql(s"SET spark.sql.shuffle.partitions=10")
      logInfo("order by 操作需要设置: SET spark.sql.shuffle.partitions=200 ")
      _sql += s" ORDER BY  ${sqlObj.orderBy} "
    }

    //暂时不支持 group by，请使用相关的Transforamtion操作

    if (Option(sqlObj.limit).isDefined)
      _sql += s" LIMIT ${sqlObj.limit}"

    logInfo(s"执行Sql:${_sql}")
    val resultRDD = sqlContext.sql(_sql).map(a => a.toSeq.toArray)

    if (Option(sqlObj.orderBy).isDefined) {
      //order by操作完成后设回默认值200
      logInfo("order by 操作完成,设回默认值: SET spark.sql.shuffle.partitions=200")
      sqlContext.sql("SET spark.sql.shuffle.partitions=200")
    }
    resultRDD
  }

  def registerTempTableIfNotExist[A <: Product : TypeTag](rdd: RDD[A], schema: String, tempTableName: String) {
    if (notExistTempTable(tempTableName)) {
      import sqlContext.implicits._
      rdd.toDF(schema.split(",") :_*).registerTempTable("full_fields_order")
      logInfo(s"注册临时表：$tempTableName")
    }
  }

  private def registerTempTableIfNotExist(inputFilePath: String, schema: String, tempTableName: String, separator: String) {
    //如果临时表未注册，就进行注册
    if (notExistTempTable(tempTableName)) {
      val fieldsNum = schema.split(",").length
      val tmpRDD = Contexts.sparkContext.textFile(inputFilePath)
        .map(_.split(separator)).cache()
      tmpRDD.map(t =>(t.length,1)).countByValue()
      logInfo(s"schema的字段是$fieldsNum,解析后的字段个数与行数的关系是:${tmpRDD.map(_.length).countByValue()}")

      val rowRDD = tmpRDD.filter(x => x.length == fieldsNum)
        .map(fields => for (field <- fields) yield field.trim)
        .map(fields => Row(fields: _*))
      sqlContext.createDataFrame(rowRDD, simpleSchema(schema)).registerTempTable(tempTableName)
      logInfo(s"注册临时表：$tempTableName")
    }
  }

  private def notExistTempTable(tempTableName: String): Boolean = {
    !sqlContext.tableNames().contains(tempTableName)
  }

  private def simpleSchema(schemaStr: String): StructType = {
    StructType(schemaStr.toLowerCase.split(",").map(fieldName => StructField(fieldName.trim, StringType, nullable = true)))
  }

}
