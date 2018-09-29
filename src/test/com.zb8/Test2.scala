package com.zb8

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HBaseSpark").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = ss.sparkContext

    val schemaString = "rowKey,name,age"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName.trim, StringType, true)))

    val rows = sc.makeRDD(List(Row("1", "jack1", "12"), Row("2", "jack2", "32"), Row("3", "jack3", "45"), Row("4", "jack4", "212"), Row("5", "jack5", "54")))
    val rowsDF: DataFrame = ss.createDataFrame(rows, schema)
    rowsDF.createOrReplaceTempView("student")

    val sql =s"""select rowKey,name,age from student"""
    val resultDF: DataFrame = ss.sql(sql)
    val schema2: StructType = resultDF.schema
    val columns: Array[String] = resultDF.columns
    resultDF.show()
    val rdd = resultDF.rdd.map(
      row => {
        val name = row.getAs[String]("name")
        println("#########"+row)
        name
      }
    )
    println(rdd)

    sc.stop()
    ss.stop()
  }
}
