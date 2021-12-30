package com.twq.spark.web.external

import com.twq.spark.web.CombinedId
import com.twq.spark.web.external.HBaseUserVisitInfoComponent.{columnFamily, hbaseConn, lastVisit, logger, targetUserTable}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import resource._


object HBaseUserVisitInfoComponent {
  private val logger = LoggerFactory.getLogger(classOf[HBaseUserVisitInfoComponent])
  private val hbaseConn = HbaseConnectionFactory.getHbaseConn

  private val targetUserTable = //NameSpace + qualifier 获取表名
    TableName.valueOf(HbaseConnectionFactory.hbaseTableNamespace,
      System.getProperty("web.etl.hbase.UserTableName", "web-user"))

  private val columnFamily = "f".getBytes("UTF-8") //columnFamily

  private val lastVisit = "V".getBytes("UTF-8") //Version? timestamps
}

/**
  * 利用hbase来实现用户访问状态信息的存取
  */
trait HBaseUserVisitInfoComponent extends UserVisitInfoComponent {
  //import HBaseUserVisitInfoComponent._

  /**
    * 根据user唯一标识批量从hbase中查询出user的访问信息
    *
    * @param ids user唯一标识列表
    * @return 每一个user的访问信息
    */
  override def retrieveUsersVisitInfo(ids: Seq[CombinedId]): Map[CombinedId, UserVisitInfo] = {
    //得到 get 'rowKey' 'columnFamily'
    val gets = ids.map(id => new Get(id.encode.getBytes("UTF-8")).addFamily(columnFamily))
    //import resource.managed为任何具有资源类型类实现的类型创建ManagedResource。这包括所有java.io.Closeable子类，
    // 以及任何具有close或dispose方法的类型。您还可以在自己的作用域中提供自己的资源类型类实现。
    //trait ManagedResource[+R] map 方法 此方法用于在资源处于打开状态时对资源执行操作
    managed(hbaseConn.getTable(targetUserTable)) map { userTable =>
      //results  对指定表Table的 'rowKey' 'columnFamily' 获取columnFamily中的数据
      val results: Array[Result] = //Result[] get(List<Get> var1) throws IOException;
        userTable.get(scala.collection.JavaConversions.seqAsJavaList(gets))
      ids zip results map { case (id, result) =>
        if (results.isEmpty) {
          None
        } else {
          //通过指定columnFamily,columnQualifier拿到最后后一个cell记录
          val lastVisitCell = result.getColumnLatestCell(columnFamily, lastVisit)
          lastVisitCell match {
            case null => logger.error("{} --- 'visit' cells were null", id); None
            case c1 => Some(id ->
              UserVisitInfo(id, c1.getTimestamp, Bytes.toInt(c1.getValueArray, c1.getValueOffset, c1.getValueLength)))
          }
        }
      }
      /*This method is used to extract the resource being managed.
      *def either: Either[Seq[Throwable], A]此方法用于提取正在管理的资源。如果不使用Option,使用Either其中左边包含在提取过程中发生的任何错误，右边包含提取的值。
      * Either
      * Either的一个常用用法是作为Option的替代，用于处理可能缺失的值。
      * 在这种用法中，None被替换为Left，它可以包含有用的信息。
      * Right取代了Some。按照惯例，失败时用左，成功时用右。
      */
    } either match {
      case Left(exceptions) =>
        logger.error("Failed to retrieve the user visit info, details:", exceptions)
        Map.empty[CombinedId, UserVisitInfo]
      case Right(result) => result.flatten.toMap
    }
  }

  //更新UserVisitInfo至指定table的HBase数据库,也就是更新访客的历史访问信息,聚合计算时充实UserVisitInfo
  //类型Seq[UserVisitInfo]的原因  PartitionProcessor ---> def run() = { val persistUserVisitInfoBuilder = Vector.newBuilder[UserVisitInfo] 聚合计算后 省略后续代码
  //persistUserVisitInfoBuilder += userVisitInfo}
  override def updateUsersVisitInfo(usersInfo: Seq[UserVisitInfo]): Unit = {
    val puts: Seq[Put] = usersInfo map { userVisitInfo =>
      val rowKey = userVisitInfo.id.encode.getBytes("UTF-8")
      val put = new Put(rowKey)
      put.addColumn(columnFamily, lastVisit,
        userVisitInfo.lastVisitTime, Bytes.toBytes(userVisitInfo.lastVisitIndex))
      put
    }
    for (table <- managed(hbaseConn.getTable(targetUserTable))) {
      logger.debug(s"Trying to update ${puts.size} users' visit stats , detailed stats: ${usersInfo}, put cmds: ${puts}")
      val putList = scala.collection.JavaConversions.seqAsJavaList(puts)
      try {
        table.batch(putList, Array.fill(putList.size())(new Object))
      } catch {
        case e: Exception => logger.error("Failed to do the batch puts in hbase.", e)
      }
    }

  }

}