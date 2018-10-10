package com.zb8.utils

import com.zb8.works.PvSParamtypeAndContenttype.{loadPHoenixDataAsDFByDataSource, zkAddress}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author:
  * @ Description:
  * @ Date: Created in 11:44 2018/9/30
  * @ Modified By:
  */
object PhoenixDataSourceUtils {
  /**
    * 根据sql查询一个值
    *
    * @param ss
    * @param sql
    * @param tableName
    * @return
    */
  def getSingleColValueFromPhoenix(ss: SparkSession, sql: String, tableName: String): Any = {
    val phoenixDF: DataFrame = loadPHoenixDataAsDFByDataSource(ss, tableName, zkAddress)
    phoenixDF.createOrReplaceTempView(tableName)
    val frame: DataFrame = ss.sql(sql)
    val rows: Array[Row] = frame.take(1)
    var colValue: Any = null
    if (rows.length == 1) {
      colValue = rows(0).get(0)
    }
    colValue
  }
}
