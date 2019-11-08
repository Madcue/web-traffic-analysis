package com.twq.preparser

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.SparkConf

object PreparseETL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(args.isEmpty){
      conf.setMaster("local")
    }
    val spark =
      SparkSession.builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    val rawdataInputPath = spark.conf.get("spark.traffic.analysis.rawdata.input",
    "hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180616")
    val numPartitions = spark.conf.get("spark.traffic.analysis.rawdata.numberPartitions","2").toInt
    val preParsedLogRDD = spark.sparkContext.textFile(rawdataInputPath)
      .flatMap(line =>Option(WebLogPreParser.parse(line)))
   val preParsedLogDS= spark.createDataset(preParsedLogRDD)(Encoders.bean(classOf[PreParsedLog]))

    preParsedLogDS.coalesce(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("year","month","day")
      .saveAsTable("rawdata.web")

    spark.stop()

  }

}
