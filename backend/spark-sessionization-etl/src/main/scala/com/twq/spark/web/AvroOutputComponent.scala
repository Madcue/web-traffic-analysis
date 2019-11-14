package com.twq.spark.web

import com.twq.spark.web.session.DataRecords
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection._

/**
  *  ETL输出组件(以Avro的文件格式输出到HDFS)
  */
trait AvroOutputComponent {

  private val avroWriters = mutable.HashMap.empty[String, DataFileWriter[GenericRecord]]

  /**
    *  将ETL计算出来的实体以Avro文件格式输出到HDFS中
    * @param dataRecords  已经计算好的实体
    * @param outputBasePath 输出位置信息
    * @param dateStr  处理的数据的日期
    * @param partitionIndex 第几个分区(分区的Index)
    */
  def writeDataRecords(dataRecords: Seq[DataRecords],
                       outputBasePath: String, dateStr: String, partitionIndex: Int) = {
    val recordsMap = toRecordsMap(dataRecords)
    AvroRecordBuilder.SCHEMAS.foreach { case (key, schema) =>
      //写每种实体类名对应的所有实体到HDFS中
      recordsMap.get(key) foreach { records =>
        if (records.nonEmpty) {
          val writerKey = key
          //根据实体类名拿到avroWriter，没有的话则创建对应的avroWriter
          val writer = avroWriters.getOrElseUpdate(writerKey,
            createAvroWriter(key, outputBasePath, dateStr, partitionIndex, schema))
          //将一个实体类名对应的所有的实体写入到HDFS中
          records.foreach(writer.append)
        }
      }
    }
  }

  /**
    *  初始化一个avro的writer
    * @param objectType 实体类名(即实体类型)
    * @param outputBaseDir 输出路径
    * @param dateStr  数据的日期
    * @param partitionIndex 分区index
    * @param schema 实体对应的schema
    * @return AvroWriter
    */
  private def createAvroWriter(objectType: String, outputBaseDir: String,
                               dateStr: String, partitionIndex: Int, schema: Schema):
  DataFileWriter[GenericRecord] = {
    val (year, month, day) = (dateStr.take(4), dateStr.take(6), dateStr)
    val pathString = s"${outputBaseDir}/${objectType.toLowerCase}/year=$year/month=$month/day=$day/part-r-${partitionIndex}.avro"
    val path = new Path(pathString)
    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    if (hdfs.exists(path)) {
      hdfs.delete(path, false)
    }
    val outputStream = hdfs.create(path)
    val writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]()).setSyncInterval(100)
    writer.setCodec(CodecFactory.snappyCodec())
    writer.create(schema, outputStream)
    writer
  }

  /**
    *  将所有的实体按照实体类名进行归类
    * @param dataRecords 需要分类的所有的实体
    * @return 返回Map(实体类名 -> 对应的所有的实体列表)
    */
  private def toRecordsMap(dataRecords: Seq[DataRecords]): Map[String, Seq[GenericRecord]] = {
    val sessions = dataRecords.map(_.sessionRecord)
    val pageViews = dataRecords.flatMap(_.pageViewRecords)
    val heartbeats = dataRecords.flatMap(_.heartbeats)
    val mouseClicks = dataRecords.flatMap(_.mouseClickRecords)
    val conversions = dataRecords.flatMap(_.conversionRecords)

    immutable.HashMap(
      AvroRecordBuilder.SESSION -> sessions,
      AvroRecordBuilder.PAGE_VIEW -> pageViews,
      AvroRecordBuilder.HEARTBEAT -> heartbeats,
      AvroRecordBuilder.MOUSE_CLICK -> mouseClicks,
      AvroRecordBuilder.CONVERSION -> conversions
    )
  }

}
