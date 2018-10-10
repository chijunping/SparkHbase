package com.zb8.works

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @ Author:
  * @ Description: upsert into select 转移大表数据到另一个集群
  * @ Date:  Created in 14:09 2018/9/22
  * @ Modified  By:
  */
object PhoenixJob_copyTable {

  //vpc网络
  //val zkAddressSource = "hb-xxxxx-002.hbase.rds.aliyuncs.com,hb-xxxxx-001.hbase.rds.aliyuncs.com,hb-xxxxx-003.hbase.rds.aliyuncs.com:2181"
  //val zkAddressTarget = "hb-xxxxx-002.hbase.rds.aliyuncs.com,hb-xxxxx-001.hbase.rds.aliyuncs.com,hb-xxxxx-003.hbase.rds.aliyuncs.com:2181"
  //经典网络
  val zkAddressSource = "hb-proxy-pub-xxxxx-002.hbase.rds.aliyuncs.com,hb-proxy-pub-xxxxx-001.hbase.rds.aliyuncs.com,hb-proxy-pub-xxxxx-003.hbase.rds.aliyuncs.com:2181"
  val zkAddressTarget = "hb-proxy-pub-xxxxx-002.hbase.rds.aliyuncs.com,hb-proxy-pub-xxxxx-001.hbase.rds.aliyuncs.com,hb-proxy-pub-xxxxx-003.hbase.rds.aliyuncs.com:2181"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PhoenixJob_copyTable")
      .setMaster("local[8]")
    val ss = SparkSession.builder().config(conf).getOrCreate()

    try {
      //load source 的表数据
      val phoenixDF: DataFrame = loadPHoenixDataAsDFByDataSource(ss, "VIEW_CLICKLOG", zkAddressSource)
      phoenixDF.createOrReplaceTempView("VIEW_CLICKLOG")
      val frame: DataFrame = ss.sql("SELECT ID,TIME,PARAM_TYPE,URL,TITLE,CONTENT_TYPE FROM VIEW_CLICKLOG WHERE time>='2018100101' AND time <'2018100102'")
      //save 数据到 target 表中
      saveDF2Phoenix(frame, "ZB8_CLICKLOG_TEST", zkAddressTarget)

    } catch {
      case e: Exception => {
        println(e.getMessage, e)
        System.exit(1)
      }
    } finally {
      ss.stop()
    }
  }

  /**
    * load data from phoenix
    *
    * @param ss
    * @param tableName
    * @param zkAddress
    * @return
    */
  def loadPHoenixDataAsDFByDataSource(ss: SparkSession, tableName: String, zkAddress: String): DataFrame = {
    val phoenixDF: DataFrame = ss.read
      .options(Map("table" -> tableName, "zkUrl" -> zkAddress))
      .format("org.apache.phoenix.spark")
      .load
    phoenixDF
  }

  /**
    * save dataFrame to phoenix
    *
    * @param df
    * @param tableName
    * @param zkAddress
    */
  def saveDF2Phoenix(df: DataFrame, tableName: String, zkAddress: String) = {
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> tableName, "zkUrl" -> zkAddress))
      .save()
  }
}