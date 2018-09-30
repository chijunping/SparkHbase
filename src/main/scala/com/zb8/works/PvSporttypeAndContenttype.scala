package com.zb8.works

import java.util
import java.util.{Calendar, Date}

import com.zb8.utils.{PhoenixJDBCUtil, TimeUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @ Author:
  * @ Description:
  * @ Date:  Created in 14:09 2018/9/22
  * @ Modified  By:
  */
object PvSporttypeAndContenttype {

  //vpc网络
    val zkAddress = "hb-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";
  //经典网络
//  val zkAddress = "hb-proxy-pub-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";
  val phoenixJdbcUrl = "jdbc:phoenix:" + zkAddress
  PhoenixJDBCUtil.setPhoenixJDBCUrl("jdbc:phoenix:" + zkAddress)

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("执行main方法时，入参个数错误。")
      System.exit(1)
    }
    val timeStep: Int = args(0).toInt
    val conf = new SparkConf().setAppName("PvSporttypeAndContenttype")
//      .setMaster("local[8]")
    val ss = SparkSession.builder().config(conf).getOrCreate()

    val newTimes: util.List[String] = getNewTime(timeStep)
    val newStartTime = newTimes.get(0)
    val newEndTime = newTimes.get(1)
    var phoenixDF: DataFrame = null
    try {
      phoenixDF = loadPHoenixDataAsDFByDataSource(ss, "ZB8_CLICKLOG", zkAddress)
      phoenixDF.createOrReplaceTempView("ZB8_CLICKLOG")
      val sql =
        s"""
           |SELECT '${newEndTime}' as ENDTIME,'${newStartTime}' as STARTTIME,PLATFORM,PARAM_TYPE as SPORT_TYPE,CONTENT_TYPE,COUNT(*) as FREQUENCY
           |FROM ZB8_CLICKLOG
           |WHERE time>='${newStartTime}' AND time<'${newEndTime}' AND PARAM_TYPE in('basketball','football','other')
           |GROUP BY PLATFORM,PARAM_TYPE,CONTENT_TYPE
         """.stripMargin
      val phoenixDFInTime = ss.sql(sql) //.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //将结果存入hbase
      saveDF2Phoenix(phoenixDFInTime, "ZB8_STAT_SPORTTYPE_CONTENTTYPE", zkAddress)
      //存入最近计算时间
      val sss = s"upsert into ZB8_STAT_SPORTTYPE_CONTENTTYPE(ENDTIME,STARTTIME) values('${newEndTime}','${newStartTime}')"
      PhoenixJDBCUtil.insert(sss, Array[AnyRef]())
    } catch {
      case e: Exception => {
        println(e.getMessage, e)
        System.exit(1)
      }
    } finally {
      //if (phoenixDF != null) phoenixDF.unpersist()
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

  /**
    * get newStatTimes: newStartTime、newEndTime
    *
    * @param timeStep
    * @return
    */
  def getNewTime(timeStep: Int): util.List[String] = {
    val sql_maxEndTime: String = "SELECT  ENDTIME \"maxEndTime\" FROM ZB8_STAT_SPORTTYPE_CONTENTTYPE ORDER BY ENDTIME DESC LIMIT 1"
    var newStartTime: String = PhoenixJDBCUtil.queryForSingleColumIgnoreCase(sql_maxEndTime, null)
    if (newStartTime == null) { //newStartTime = TimeUtils.date2DateStr(new Date(), "yyyyMMddHHmm");
      newStartTime = "2018092716"
    }
    val newStartTimeDate: Date = TimeUtils.dateStr2Date(newStartTime, "yyyyMMddHH")
    val cal: Calendar = Calendar.getInstance
    cal.setTime(newStartTimeDate)
    cal.add(Calendar.HOUR_OF_DAY, timeStep)
    val endTimeInMillis: Long = TimeUtils.dateStr2TimeStemp(newStartTime, "yyyyMMddHH")
    if ((System.currentTimeMillis - endTimeInMillis) < 3600000) {
      println("暂无最新数据需要计算，已将现有数据计算完成。")
      System.exit(0)
    }
    val newEndTime: String = TimeUtils.timeStemp2DateStr(String.valueOf(cal.getTimeInMillis), "yyyyMMddHH")
    util.Arrays.asList(newStartTime, newEndTime)
  }

}