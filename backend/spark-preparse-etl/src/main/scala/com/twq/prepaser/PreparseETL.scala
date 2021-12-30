package com.twq.prepaser

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

/**
  * export HADOOP_CONF_DIR=/home/hadoop-twq/bigdata/hadoop-2.7.5/etc/hadoop/
  * spark-submit --master yarn \
  * --class com.twq.prepaser.PreparseETL \
  * --driver-memory 512M \
  * --executor-memory 512M \
  * --num-executors 2 \
  * --executor-cores 1 \
  * --conf spark.traffic.analysis.rawdata.input=hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180617 \
  * /home/hadoop-twq/traffice-analysis/jars/spark-preparse-etl-1.0-SNAPSHOT-jar-with-dependencies.jar prod
  */
//预解析日志ETL
object PreparseETL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("PreparseETL")
    //spark sql etl
    val spark = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()

    val rawdataInputPath = conf.get("spark.traffic.analysis.rawdata.input",
      "hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180616")

    val numberPartitions = conf.get("spark.traffic.analysis.rawdata.numberPartitions", "2").toInt
    //Spark功能的主要入口点。SparkContext表示与Spark集群的连接，可以用来在该集群上创建rdd、累加器和广播变量
    //原始日志无强类型
    val preParsedLogRDD: RDD[PreParsedLog] = spark.sparkContext.textFile(rawdataInputPath)
      .flatMap(line => Option(WebLogPreParser.parse(line)))
    val preParsedLogDS: Dataset[PreParsedLog] = spark.createDataset(preParsedLogRDD)(Encoders.bean(classOf[PreParsedLog]))
    //to hive ,hive表给定了年 月 日分区
    // 所有在hive表的真正存储位置是HDFS里的文件名表示为../hive/warehouse/...db/web/year=xxxx/month=xxxxxx/day=20201006这样的
    preParsedLogDS.coalesce(numberPartitions)
      .write
      .partitionBy("year","month","day")
      .saveAsTable("rawdata.web")

    spark.stop()
  }
}
