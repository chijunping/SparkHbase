package com.zb8

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author: 19shusheng.com
  * @ Description:
  * @ Date: Created in 10:35 2018/9/20
  * @ Modified By:
  */
object test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ZB8_SparksqlOnPhoenix").setMaster("local[4]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext

    val words = Array(Row("001", "皇马,火箭,哈德"), Row("002", "皇马,火箭,哈德"), Row("001", "飞机,火箭,哈德"), Row("001", "飞机,赞皇,哈德"))
    val value: RDD[Row] = sc.parallelize(words)

    value.flatMap(row => {
      val id: String = row.getString(0)
      val label: String = row.getString(1)
      val labelArr: Array[String] = label.split(",")
      labelArr
    }).map((_, 1))
      .reduceByKey(_ + _)
      .foreach(println)
    //    resultDF.rdd.take(3).map(row => {
    //      val id = row.get(0)
    //      val strings = row.get(1).toString.split(",")
    //
    //    })
    ss.stop()
    sc.stop()
  }
}
