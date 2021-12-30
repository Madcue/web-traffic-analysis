package com.twq.spark.web

import com.twq.prepaser.PreParsedLog
import com.twq.spark.web.external.{HBaseSnapshotAdmin, HbaseConnectionFactory}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Encoders, Row, SparkSession}


/**
  * Hue
  * 网站流量离线分析Spark ETL入口(应用层面)
  * standalone Spark Job
  * spark-submit --master spark://master:7077 \
  * --class com.twq.spark.web.WebETL \
  * --driver-memory 512M \
  * --executor-memory 1G \
  * --total-executor-cores 2 \
  * --executor-cores 1 \
  * --conf spark.web.etl.inputBaseDir=hdfs://master:9999/user/hive/warehouse/rawdata.db/web \
  * --conf spark.web.etl.outputBaseDir=hdfs://master:9999/user/hadoop-twq/traffic-analysis/web \
  * --conf spark.web.etl.startDate=20180617 \
  * --conf spark.driver.extraJavaOptions="-Dweb.metadata.mongodbAddr=192.168.1.102 -Dweb.etl.hbase.zk.quorums=master" \
  * --conf spark.executor.extraJavaOptions="-Dweb.metadata.mongodbAddr=192.168.1.102 -Dweb.etl.hbase.zk.quorums=master -Dcom.sun.management.jmxremote.port=1119 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" \
  * /home/hadoop-twq/traffice-analysis/jars/spark-sessionization-etl-1.0-SNAPSHOT-jar-with-dependencies.jar prod
  */

object WebETL {
  def main(args: Array[String]): Unit = {
    /**
      * 大多数时候，你会用新的SparkConf()创建一个SparkConf对象，它会从任何spark加载值。* Java系统属性设置在您的应用程序以及。
      * 在这种情况下，直接在SparkConf对象上设置的参数优先于系统属性。
      */
    val conf = new SparkConf
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[4]")
    }
    //注意getName和getSimpleName不同 getName从包到名
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[WebRegistrator].getName)

    //预处理输出的基本路径,hive路径
    val wdPreparsedLogBaseDir = conf.getOption("spark.web.etl.inputBassDir")
      .getOrElse("hdfs://master:9999/user/hive/warehouse/rawdata.db/web") //the option's value if the option is nonempty, otherwise return the result of evaluating default.
    //主ETL输出路径
    val outputBaseDir = conf.getOption("spark.web.etl.outputBaseDir")
      .getOrElse("hdfs://master:9999/user/hadoop-twq/traffic-analysis/web")
    //日志的时间
    val dateStr = conf.getOption("spark.web.etl.startDate").getOrElse("20180616")
    //分区数
    val numPartitions = conf.getInt("spark.web.etl.numberPartitions", 5)

    conf.setAppName(s"WebETL-${dateStr}")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    //预处理输出的具体路径
    val preParsedLogPath =
      s"${wdPreparsedLogBaseDir}/year=${dateStr.substring(0, 4)}/month=${dateStr.substring(0, 6)}/day=${dateStr}"
    /**
      * transform后数据为：
      * PreParsedLog(profileId1, pv, queryString, serverTime....)
      * PreParsedLog(profileId1, mc, queryString, serverTime....)
      * PreParsedLog(profileId2, pv, queryString, serverTime....)
      * ..........
      * PreParsedLog(profileId3, ev, queryString, serverTime....)
      * PreParsedLog(profileIdn, hb, queryString, serverTime....)
      */
    //读取预处理后的日志,强类型RDD[PreParsedLog]
    //  DataFrame to RDD
    val parsedLogRDD = spark.read.parquet(preParsedLogPath)
      //将DataFrame转成Dataset[PreParsedLog]再转成RDD[PreparsedLog]
      //dataset中的map操作
      //Returns a new Dataset that contains the result of applying func to each element.
      //在spark sql操作中,scala环境使Java对象用Encoders.bean()这种静态工厂方法(为类型T的Java Bean创建一个编码器。)
      //将Java Beans构建为Encoders 它是Encoder的实现(创建)-(schema + class tag)Used to convert a JVM object of type T to and from the internal Spark SQL representation
      .map(transform(_))(Encoders.bean(classOf[PreParsedLog])).rdd
      //将RDD[PreParsedLog]转成RDD[(CombinedId, BaseDataObject)]
      .flatMap(p => WebLogParser.parse(p)) //一个PreParsedLog可能会产生多个各种类型的xxDataObject所以展平
    /**
      * WebLogParser.parse转换后数据为：
      * (CombinedId(profileId1,user1), BaseDataObject(profileId1,user1,pv,client_ip.....))
      * (CombinedId(profileId1,user1), BaseDataObject(profileId1,user1,mc,client_ip.....))
      * (CombinedId(profileId2,user2), BaseDataObject(profileId2,user2,pv,client_ip.....))
      *  ............
      *  (CombinedId(profileId3,user3), BaseDataObject(profileId3,user3,ev,client_ip.....))
      *  (CombinedId(profileIdn,usern), BaseDataObject(profileIdn,usern,pv,client_ip.....))
      */
    //将parsedLogRDD按照key进行分组 ,
    // mapPartitionsWithIndex 函数参数index:跟踪的原始分区的索引,第二个参数输入函数是否保留分区preservesPartitioning: Boolean = false,
    // 前提PairRDD 且 函数不对key修改才设置true
    parsedLogRDD.groupByKey(new HashPartitioner(numPartitions)).mapPartitionsWithIndex((index,iterator) => {
      //groupByKey后的每一个分区的数据为：
      /**
        * 转换后数据为：
        * (CombinedId(profileId1,user1), List(BaseDataObject(profileId1,user1,pv,client_ip.....),
        *                                     BaseDataObject(profileId1,user1,mc,client_ip.....)))
        * (CombinedId(profileId2,user2), List(BaseDataObject(profileId2,user2,pv,client_ip.....),
        *                                     BaseDataObject(profileId2,user2,mc,client_ip.....),
        *                                     BaseDataObject(profileId2,user2,ev,client_ip.....)
        *                                     BaseDataObject(profileId2,user2,hb,client_ip.....)))
        *  ............
        *  (CombinedId(profileId3,user3), List(BaseDataObject(profileId3,user3,ev,client_ip.....)))
        *  (CombinedId(profileIdn,usern), List(BaseDataObject(profileIdn,usern,pv,client_ip.....),
        *                                      BaseDataObject(profileIdn,usern,mc,client_ip.....),
        *                                      BaseDataObject(profileIdn,usern,pv,client_ip.....)))
        */
      //处理每一个分区的数据  iterator:Iterator[(CombinedId, Iterable[BaseDataObject])]
      val partitionProcessor = new PartitionProcessor(index, iterator, outputBaseDir, dateStr)
      partitionProcessor.run()
      //RDD[U]是返回类型,但这里不需要返回，所以返回一个Iterator封装空 //final abstract class Unit private extends AnyVal
      Iterator[Unit]()
      //前面是transform操作RDD的pipeline链 foreach触发action操作提交job->stage->task运算出partitionProcessor
    }).foreach( (_: Unit) => {})

    spark.stop()

    //给表web-user创建snapshot，以便于数据的重跑
    val snapshotAdmin = new HBaseSnapshotAdmin(HbaseConnectionFactory.getHbaseConn)
    val targetUserTable = System.getProperty("web.etl.hbase.UserTableName", "web-user")
    snapshotAdmin.takeSnapshot(s"${targetUserTable}_snapshot-${dateStr}", targetUserTable)
  }


  // 为什么要transform,因为PreparseETL对原始日志调用class WebLogPreParser.parse
  // 返回出来的PreParsedLog 充实带有 private int year; private int month; private int day;
  // 用于我们的hive表的分区,PreparseETL中spark session用partitioned by指定创建的分区，多个分区意味着多级目录,输出到Hive表,在HDFS中存储
  // 而这里我们不需要year month day 去充实PreParsedLog,所以先对之前的PreParsedLog转化 去掉year month day字段后充实PreParsedLog,这就是业务剥离操作.
  private def transform(row: Row): PreParsedLog = {
    val p = new PreParsedLog
    p.setClientIp(row.getAs[String]("clientIp"))
    p.setCommand(row.getAs[String]("command").toString)
    p.setMethod(row.getAs[String]("method"))
    p.setProfileId(row.getAs[Int]("profileId"))
    p.setQueryString(row.getAs[String]("queryString"))
    p.setServerIp(row.getAs[String]("serverIp"))
    p.setServerPort(row.getAs[Int]("serverPort"))
    p.setServerTime(row.getAs[String]("serverTime"))
    p.setUriStem(row.getAs[String]("uriStem"))
    p.setUserAgent(row.getAs[String]("userAgent"))
    p
  }
}