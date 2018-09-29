package com.zb8

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark读写phoenix
  * 读：三种方式，一种RDD，两种dataFrame
  * 写：两种方式，RDD->phoenix(可自定义列名)；dataFrame->phoenix(用到case class定义消息体时，隐式转换会将所有字段转成大写)
  */
object SparkPhoenix {

  val zkAddress = "hb-proxy-pub-bp1987l1fy04etj46-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp1987l1fy04etj46-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp1987l1fy04etj46-003.hbase.rds.aliyuncs.com:2181"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkPhoenix").setMaster("local[4]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext

    /*
    //1.使用Data Source API加载Phoenix数据成 DF
    val phoenixDF: DataFrame = loadPHoenixDataAsDFByDataSource(ss)
    phoenixDF.show(10)
    */

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


    sc.stop()
    ss.stop()
  }

  /**
    * 1.使用Data Source API加载Phoenix数据成 DF
    */
  def loadPHoenixDataAsDFByDataSource(ss: SparkSession): DataFrame = {
    val phoenixDF: DataFrame = ss.read
      .options(Map("table" -> "\"clickLog_dev\"", "zkUrl" -> zkAddress))
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
    val sqlContext: SQLContext = ss.sqlContext
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", zkAddress)

    sqlContext.phoenixTableAsDataFrame(
      table = "\"clickLog_dev\"",
      columns = Array("UDID", "event", "time"),
      conf = configuration
    )
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
