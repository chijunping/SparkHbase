package com.zb8

import java.util
import java.util.{Calendar, Date}

import com.zb8.utils.{PhoenixJDBCUtil, TimeUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * @ Author:
  * @ Description:
  * @ Date:  Created in 14:09 2018/9/22
  * @ Modified  By:
  */
object SparkOnPhoenix_calculateLabels_realTest {

  //  val phoenixJdbcUrl = "jdbc:phoenix:hb-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";
  val phoenixJdbcUrl = "jdbc:phoenix:hb-proxy-pub-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";
  val zkAddress = "hb-proxy-pub-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";
  PhoenixJDBCUtil.setPhoenixJDBCUrl(phoenixJdbcUrl)

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.exit(1)
    }

    val timeStep: Int = args(0).toInt
    val conf = new SparkConf().setAppName("calculateLabels")
      .setMaster("local[8]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext

    //    val timeToday = TimeUtils.date2DateStr(new Date(), "yyyyMMdd")
    //    val timeNextDay = TimeUtils.timeStemp2DateStr(TimeUtils.addTime(timeToday, "yyyyMMdd", TimeUtils.DAY, 1) + "", "yyyyMMdd")
    val newTimes: util.List[String] = getNewTime(timeStep)
    val newStartTime = newTimes.get(0)
    val newEndTime = newTimes.get(1)
    //1.使用jdbc加载Phoenix数据成 DF
    //自定义SQL加载数据（sql整体需要用()括起来）
    val imsiTableName =
    s"""
       |(
       |SELECT
       |/*+ INDEX(ZB8_CLICKLOG IDX_ZB8_CLICKLOG_TIME_MODEL_EVENT)*/
       |CONTENT_LABEL
       |FROM ZB8_CLICKLOG
       |WHERE time>='${newStartTime}' and time<'${newEndTime}'
       |)
        """.stripMargin
    //
    //根据sql加载数据成DF
    var phoenixDF: DataFrame = null
    try {
      //      phoenixDF = loadPHoenixDataAsDFByDataSource(ss, "ZB8_CLICKLOG", zkAddress)
      phoenixDF = loadPHoenixDataAsDFByJDBC(imsiTableName, ss).persist(StorageLevel.MEMORY_AND_DISK)
      //      phoenixDF = loadPHoenixDataAsDFByDF(ss)

      phoenixDF.createOrReplaceTempView("ZB8_CLICKLOG")
      val sql =
        s"""
           |select CONTENT_LABEL from ZB8_CLICKLOG where CONTENT_LABEL is not null
         """.stripMargin
      val phoenixDFInTime = ss.sql(sql).persist(StorageLevel.MEMORY_AND_DISK_SER)
      phoenixDFInTime.rdd.map(row => {
        val CONTENT_LABEL: String = row.getAs[String]("CONTENT_LABEL")
        val TIME: String = row.getAs[String]("TIME")
        (TIME, CONTENT_LABEL)
      })
      val labelWCRDD: RDD[(String, String, String, Int)] = phoenixDFInTime.rdd
        .flatMap(row => {
          var CONTENT_LABEL: String = row.getString(0)
          if (CONTENT_LABEL == null) {
            CONTENT_LABEL = ""
          }
          CONTENT_LABEL.split(",")
        })
        .map((_, 1))
        .reduceByKey(_ + _)
        //.persist(StorageLevel.DISK_ONLY)
        .map(line => {
        val word = line._1
        val count = line._2
        (newEndTime, newStartTime, word, count)
      })
      //.persist(StorageLevel.DISK_ONLY)
      //labelWCRDD.foreach(println)

      //将结果存入hbase
      labelWCRDD.saveToPhoenix(
        tableName = "ZB8_STAT_LABEL",
        cols = Seq("ENDTIME", "STARTTIME", "WORD", "FREQUENCY"), //列名严格区分大小写，程序不会转换成大写
        zkUrl = Some(phoenixJdbcUrl)
      )
    } catch {
      case e: Exception => {
        println(e.getMessage, e)
        System.exit(1)
      }
    } finally {
      if (phoenixDF != null) phoenixDF.unpersist()
      sc.stop()
      ss.stop()
    }
  }


  /**
    * 使用jdbc的方式：根据 自定义SQL 从phoenix导入数据（这样可强制使用索引,处理过程太慢了(手动停止任务),spark执行计划中task任务较多(1个)，且hbase的cpu消耗不高)）
    *
    * @param ss
    */
  def loadPHoenixDataAsDFByJDBC(imsiTableName: String, ss: SparkSession): DataFrame = {

    val phoenixDF: DataFrame = ss.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", phoenixJdbcUrl)
      .option("dbtable", imsiTableName)
      .load
    phoenixDF
  }


  /**
    * 3.phoenixTableAsDataFrame方法(处理过程太慢了(手动停止任务),spark执行计划中task任务较多(160个)，且hbase的cpu消耗较高)
    */
  def loadPHoenixDataAsDFByDF(ss: SparkSession): DataFrame = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", zkAddress)
    val phoenixDF: DataFrame = ss.sqlContext.phoenixTableAsDataFrame(
      table = "ZB8_CLICKLOG",
      columns = Array("TIME", "CONTENT_LABEL"),
      conf = configuration
    )
    phoenixDF
  }

  /**
    * 1.使用Data Source API加载Phoenix数据成 DF（处理17.5个小时的数据用时20分钟，spark执行计划中task任务较多(160个),hbase的cpu占用不高）
    * （暂时推荐使用，优于其它两种方式）
    */
  def loadPHoenixDataAsDFByDataSource(ss: SparkSession, tableName: String, zkAddress: String): DataFrame = {
    val phoenixDF: DataFrame = ss.read
      .options(Map("table" -> tableName, "zkUrl" -> zkAddress))
      .format("org.apache.phoenix.spark")
      .load
    phoenixDF
  }

  /**
    * 获取 ZB8_STAT_LABEL表时间字段最大的值
    *
    * @return
    */
  def getNewTime(timeStep: Int): util.List[String] = {
    val sql_maxEndTime: String = "SELECT  ENDTIME \"maxEndTime\" FROM ZB8_STAT_LABEL ORDER BY ENDTIME DESC LIMIT 1"
    var newStartTime: String = PhoenixJDBCUtil.queryForSingleColumIgnoreCase(sql_maxEndTime, null)
    if (newStartTime == null) { //newStartTime = TimeUtils.date2DateStr(new Date(), "yyyyMMddHHmm");
      newStartTime = "201809271700"
    }
    val newStartTimeDate: Date = TimeUtils.dateStr2Date(newStartTime, "yyyyMMddHHmm")
    val cal: Calendar = Calendar.getInstance
    cal.setTime(newStartTimeDate)
    cal.add(Calendar.MINUTE, timeStep)
    val endTimeInMillis: Long = cal.getTimeInMillis
    if ((System.currentTimeMillis - endTimeInMillis) < 0) {
      println("暂无最新数据需要计算，已将现有数据计算完成。")
      System.exit(0)
    }
    val newEndTime: String = TimeUtils.timeStemp2DateStr(String.valueOf(cal.getTimeInMillis), "yyyyMMddHHmm")
    util.Arrays.asList(newStartTime, newEndTime)
  }

}