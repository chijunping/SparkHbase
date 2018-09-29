package com.zb8.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.mapred.JobConf

/**
  * Created by Duwei on 2017/4/11.
  */
object CommUtil {


  def getTable(name: String): Table = {
    val configura = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(configura)
    val table = conn.getTable(TableName.valueOf(name))
    table
  }

  def timeFormat(): String = {
    val date = new Date()
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(date)
  }

  def timeParse(time: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(time).getTime
  }

  def getHbaseJobConf(tableName_output: String, zkAddress: String): JobConf = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddress)
    val jobConf = new JobConf(hBaseConf)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Result])
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName_output)
    jobConf
  }
}
