package com.zb8

import java.util

import com.zb8.utils.CommUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * spark通过 TableInputFormat 和 TableOutputFormat 来读写hbase
  */
object SparkHbase {

  val zkAddress = "hb-proxy-pub-xxxxx-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-xxxxx-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-xxxxx-003.hbase.rds.aliyuncs.com:2181"
  val table_input = "bigdata:click"
  val table_output = "bigdata:click_dev"
  val scan_row_start = "26_86108"
  val scan_row_stop = "26_861084038826964_20180612105450046"
  val scan_column_family = "info"
  val scan_columns = "info:click"


  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setMaster("local[4]").setAppName("HBaseSpark").setJars(List("target/spark4hbase.jar"))
    val conf = new SparkConf().setAppName("HBaseSpark").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = ss.sparkContext
    //配置HbaseConfig
    val hBaseConf: Configuration = getHbaseConf
    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val count: Long = hbaseRDD.count()
    val rowKeyRDD: RDD[String] = hbaseRDD.map(tuple => tuple._1).map(item => Bytes.toString(item.get()))
    //val count1: Long = rowKeyRDD.count()
    // rowKeyRDD.take(3).foreach(println)
    val resultRDD: RDD[Result] = hbaseRDD.map(tuple => tuple._2).cache()
    //val count2: Long = resultRDD.count()
    //操作hbaseRDD 整理返回自己想要的 RDD格式,这里整理成org.apache.spark.sql.Row(colum01,colum02,colum03)
    val keyValueRDD = resultRDD.map(result => {
      var results = Array(result)
      val maps: util.List[util.Map[String, String]] = toMapsFromResultArr(results)
      val map: util.Map[String, String] = maps.get(0)
      val values: util.Collection[String] = map.values()
      //list转Array并知道arra的泛型为String
      values.toArray(new Array[String](values.size))
    }).filter(array => array.length == 21)
      .map(arr =>
        org.apache.spark.sql.Row(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10), arr(11), arr(12), arr(13), arr(14), arr(15), arr(16), arr(17), arr(18), arr(19), arr(20), arr(20), arr(20), arr(20), arr(20), arr(20))
      ).cache() //这里测试结果：可超过26个字段，不受Tuple20 限制

    //准备 createDataFrame的 schema
    val schemaString = "col1,col2,col3,col4,col5,col6,col7,col8,col9," +
      "col10," +
      "col11," +
      "col12," +
      "col13," +
      "col14," +
      "col15," +
      "col16," +
      "col17," +
      "col18," +
      "col19," +
      "col20," +
      "col21," +
      "col22," +
      "col23," +
      "col24," +
      "col25," +
      "col26"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName.trim, StringType, true)))
    //RDD[Row] 转DataFrame,并注册成临时表
    val keyValueDF: DataFrame = ss.createDataFrame(keyValueRDD, schema)
    keyValueDF.createOrReplaceTempView("table1")
    //SparkSQL 统计；分析需求：根据col8列进行分组统计，列出每组中最接近当前时间的前三条记录
    var sql =
      s"""
         |select * from
         |(
         |SELECT col1,col2,col3,col6,col8,row_number() over(PARTITION BY col8 ORDER by col6 DESC) as rank FROM table1
         |)
         |WHERE rank <= 3
         |ORDER BY col2 desc
       """.stripMargin
    val resultDF: DataFrame = ss.sql(sql).cache()
    resultDF.show()
    // 把分析结果存到Hbase(或者Redis\Mysql等)
    val jobConf: JobConf = CommUtil.getHbaseJobConf(table_output, zkAddress)
    //sql.DataFrame转 hbase.client.Put
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = resultDF2Puts(resultDF).cache()
    // val count3: Long = putsRDD.count()
    putsRDD.saveAsHadoopDataset(jobConf)
    sc.stop()
    ss.stop
  }


  private def getHbaseConf: Configuration = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddress)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, table_input)
    hBaseConf.set(TableInputFormat.SCAN_ROW_START, scan_row_start)
    hBaseConf.set(TableInputFormat.SCAN_ROW_STOP, scan_row_stop)
    //hBaseConf.set(TableInputFormat.SCAN_COLUMNS, scan_columns)
    hBaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, scan_column_family)
    hBaseConf
  }

  private def resultDF2Puts(resultDF: DataFrame): RDD[(ImmutableBytesWritable, Put)] = {
    val schema2: StructType = resultDF.schema
    val columns: Array[String] = resultDF.columns
    val putsRDD = resultDF.rdd.map(row => {
      var map: Map[String, String] = row.getValuesMap(columns)
      val uuid: Option[String] = map.get("col1")
      val rowkey: String = uuid.toString
      val put = new Put(Bytes.toBytes(rowkey))
      import scala.collection.JavaConversions._
      for (entry <- map.entrySet) {
        val rowkey: String = map.remove("rowkey")
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey), Bytes.toBytes(String.valueOf(entry.getValue)))
      }
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    })
    putsRDD
  }

  /**
    * 将 Array[Result] 转换成 List[Map]
    *
    * @param results
    * @return
    */
  @throws[Exception]
  def toMapsFromResultArr(results: Array[Result]): util.List[util.Map[String, String]] = {
    if (results == null && results.length == 0) return null
    val recordList: util.List[util.Map[String, String]] = new util.ArrayList[util.Map[String, String]]
    for (result <- results) {
      val cells: util.List[Cell] = result.listCells
      val record: util.Map[String, String] = new util.TreeMap[String, String]
      if (cells != null && cells.size > 0) {
        for (i <- 0 until cells.size - 1) {
          val key: String = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)))
          val value: String = Bytes.toString(CellUtil.cloneValue(cells.get(i)))
          record.put(key, value)
        }
        recordList.add(record)
      }
    }
    recordList
  }

}
