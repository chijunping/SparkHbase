package com.zb8

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author: 19shusheng.com
  * @ Description:
  * @ Date:  Created in 14:09 2018/8/22
  * @ Modified  By:
  */
object ZB8_SparksqlOnPhoenix {

  val zkAddress = "hb-proxy-pub-bp151dhf9a35tg4f4-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp151dhf9a35tg4f4-003.hbase.rds.aliyuncs.com:2181";

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ZB8_SparksqlOnPhoenix").setMaster("local[4]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext

    //        val timeToday = TimeUtils.date2DateStr(new Date(), "yyyyMMdd")
    //        val timeNextDay = String.valueOf(Integer.valueOf(timeToday) + 1)
    val timeToday =   "20180119"
    val timeNextDay = "20180120"
    //1.使用jdbc加载Phoenix数据成 DF
    //自定义SQL加载数据（sql整体需要用()括起来）
    val imsiTableName =
    s"""
       |(
       |SELECT
       |/*+ INDEX(ZB8_CLICKLOG IDX_ZB8_CLICKLOG_TIME_MODEL_EVENT)*/
       |CONTENT_LABEL
       |FROM ZB8_CLICKLOG
       |WHERE time>='${timeToday}' and time<'${timeNextDay}'
       |)
          """.stripMargin
    //根据sql加载数据成DF
    var phoenixDF: DataFrame = null
    try {
      phoenixDF = loadPHoenixDataAsDFByJDBC(imsiTableName, ss).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val labelWCRDD: RDD[(String, String, Int)] = phoenixDF.rdd
        .flatMap(row => {
          val CONTENT_LABEL: String = row.getString(0)
          val labelArr: Array[String] = CONTENT_LABEL.split(",")
          labelArr
        }).map((_, 1))
        .reduceByKey(_ + _)
        .map(line => {
          val label = line._1
          val count = line._2
          (timeToday, label, count)
        })

      //将结果存入hbase
      labelWCRDD.saveToPhoenix(
        tableName = "ZB8_STAT_LABEL",
        cols = Seq("TIME", "WORD", "FREQUENCY"), //列名严格区分大小写，程序不会转换成大写
        zkUrl = Some("jdbc:phoenix:" + zkAddress)
      )
    } catch {
      case e: Exception => {
        println(e.getMessage, e)
      }
        System.exit(1)
    } finally {
      if (phoenixDF != null) phoenixDF.unpersist()
      sc.stop()
      ss.stop()
    }



    //     resultDF.show()


    /*
    //2.phoenixTableAsRDD方法
    val mapArray: Array[Map[String, AnyRef]] = loadPHoenixDataAsRddByRDD(sc)
    mapArray.foreach(println)*/

    /*//3.phoenixTableAsDataFrame方法
    loadPHoenixDataAsDFByDF(ss).createOrReplaceTempView("clickLog_dev")
    ss.sql("select * from clickLog_dev where UDID in ('0005','0006') limit 100").show()*/

    /*
    //保存RDD 到Phoenix,并查询，检测是否插入
    saveRdd2Phoenix(sc)
    loadPHoenixDataAsDFByDataSource(ss).createOrReplaceTempView("clickLog_dev")
    ss.sql("select * from clickLog_dev where UDID in ('0005','0006') limit 100").show()*/

    /*//保存DF 到Phoenix（失败，因为phoenix中的标明，字段名都是小写，转为大写后可成功插入）
    saveDF2Phoenix(ss)
    loadPHoenixDataAsDFByDataSource(ss).createOrReplaceTempView("clickLog_dev")
    ss.sql("select * from clickLog_dev where UDID in ('0007','0008') limit 100").show()*/

    //    sc.stop()
    //    ss.stop()
  }

  /**
    * 1.使用Data Source API加载Phoenix数据成 DF
    */
  def loadPHoenixDataAsDFByDataSource(ss: SparkSession, tableName: String, zkAddress: String): DataFrame = {
    val phoenixDF: DataFrame = ss.read
      .options(Map("table" -> tableName, "zkUrl" -> zkAddress))
      .format("org.apache.phoenix.spark")
      .load
    phoenixDF
  }

  /**
    * 2.phoenixTableAsRDD方法
    */


  def loadPHoenixDataAsRddByRDD(sc: SparkContext): Array[Map[String, AnyRef]] = {
    //导包：import org.apache.phoenix.spark._
    val rdd = sc.phoenixTableAsRDD("\"clickLog_dev\"", Array("UDID", "time", "event"), zkUrl = Some(zkAddress))
    rdd.take(10)
  }

  /**
    * 3.phoenixTableAsDataFrame方法
    */
  def loadPHoenixDataAsDFByDF(ss: SparkSession): DataFrame = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", zkAddress)
    val phoenixDF: DataFrame = ss.sqlContext.phoenixTableAsDataFrame(
      table = "\"clickLog_dev\"",
      columns = Array("UDID", "event", "time"),
      conf = configuration
    )
    phoenixDF
  }

  /**
    * 使用jdbc的方式：根据 自定义SQL 从phoenix导入数据
    *
    * @param ss
    */
  def loadPHoenixDataAsDFByJDBC(imsiTableName: String, ss: SparkSession): DataFrame = {

    val phoenixDF: DataFrame = ss.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", "jdbc:phoenix:" + zkAddress)
      .option("dbtable", imsiTableName)
      .load
    phoenixDF
  }


  /**
    * 保存RDD 到Phoenix，
    */
  def saveRdd2Phoenix(sc: SparkContext) = {
    val dataSet = List(("0005", "Zhang San", "20", "70"), ("0006", "Li Si", "20", "82"), ("0005", "Wang Wu", "19", "90"))
    val datasRDD: RDD[(String, String, String, String)] = sc.parallelize(dataSet)
    datasRDD.saveToPhoenix(
      tableName = "\"clickLog_dev\"",
      cols = Seq("UDID", "event", "time", "imei"), //列名严格区分大小写，程序不会转换成大写
      zkUrl = Some(zkAddress)
    )
  }

  /**
    * 保存DF 到Phoenix，dataFrame存phoenix时经过caseClass时经过大写隐式转换，所以都会变成大写，所以phoenix建表和字段建议都用大写！！！
    */
  case class clickLogDev(UDID: String, event: String, time: String, imei: String)

  def saveDF2Phoenix(ss: SparkSession) = {
    val dataSet = List(clickLogDev("0007", "Zhang San", "20", "70"), clickLogDev("0007", "Li Si", "20", "82"), clickLogDev("0008", "Wang Wu", "19", "90"))
    val df: DataFrame = ss.createDataFrame(dataSet)
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> "\"clickLog_dev\"", "zkUrl" -> zkAddress))
      .save()
  }
}