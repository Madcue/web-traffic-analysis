package com.twq.spark.web

import com.twq.parser.dataobject.BaseDataObject
import com.twq.spark.web.session.{SessionData, SessionGenerator, UserSessionDataAggregator}
import com.twq.spark.web.external.{HBaseUserVisitInfoComponent, UserVisitInfo}

import scala.collection.mutable


/**
  * group by key后 mapPartitionWithIndex
  * *跟踪到分区的index  转换后数据为：
  * 假设index =1    (CombinedId(profileId1,user1), List(BaseDataObject(profileId1,user1,pv,client_ip.....),
  * ---------------------------------------------------BaseDataObject(profileId1,user1,mc,client_ip.....)))
  * 假设index =2    (CombinedId(profileId2,user2), List(BaseDataObject(profileId2,user2,pv,client_ip.....),
  *
  * 每一个分区数据的处理者(分区级别)
  *
  * @param index              分区的index
  * @param dataPerProfileUser 分区中的数据
  * @param outputBasePath     分区输出的路径
  * @param dateStr            处理的数据日期
  */
//构造4参
class PartitionProcessor(index: Int,
                         dataPerProfileUser: Iterator[(CombinedId, Iterable[BaseDataObject])],
                         outputBasePath: String,
                         dataStr: String) extends SessionGenerator with HBaseUserVisitInfoComponent with AvroOutputComponent {
  //一个user的开始的serverSessionId
  private var serverSessionIdStart = index.toLong + System.currentTimeMillis() * 1000

  def run() = {

    //循环处理每一个user的DataObjects 迭代器内数据分组 传值组大小
    dataPerProfileUser grouped (512) foreach { case currentBatch =>
      val ids: Seq[CombinedId] = currentBatch.map(_._1)
      //根据user唯一标识批量从hbase中查询出user的访问信息
      val usersVisitInfo: Map[CombinedId, UserVisitInfo] = retrieveUsersVisitInfo(ids)
      /**
        * vector[Java List的实现]读写能力均衡且线程安全：比如软件历史操作记录的存储，我们经常要查看历史记录；再比如，上一次的记录，上上次的记录，但却不会删除记录，因为记录是事实的描述。
        * ArrayList数组列表的使用场景：比如公交车乘客的存储，随时可能有乘客下车，支持频繁的不确定位置元素的移除 使用LinkedList插入(查慢写快)
        * deque双端队列的使用场景：比如排队购票系统，对排队者的存储可以采用deque，支持头端的快速移除，尾端的快速添加。如果采用vector,则头端移除时，会移动大量的数据，速度慢。
        * set的使用场景：比如对游戏的个人得分记录的存储，存储要求从高分到低分的顺序排列。
        * map的使用场景：比如按ID号存储十万个用户，想要快速通过ID查找对应的用户，TreeMap查找效率，这时就体现出来了。如果是vector容器，最坏的情况下可能要遍历完整个容器才能找到该用户
        *
        */
      //def newBuilder[A]: Builder[A, Vector[A]] ,Vector适用于读写性能平均的数据模型
      val persistUserVisitInfoBuilder: mutable.Builder[UserVisitInfo, Vector[UserVisitInfo]] = Vector.newBuilder[UserVisitInfo]
      //dataPerProfileUser: Iterator[(CombinedId, Iterable[BaseDataObject])]
      currentBatch foreach { case (profileUser, dataObjects) =>
        //对一个user中的所有的DataObject按照时间进行升序排序
        val sortedObjectSeq = dataObjects.toSeq.sortBy(obj => obj.getServerTime.getTime)
        //会话的切割
        val sessionData: Seq[SessionData] = groupDataObjects(sortedObjectSeq)
        //对当前user产生的会话进行聚合计算 ###
        val aggregator: UserSessionDataAggregator = new UserSessionDataAggregator(profileUser, serverSessionIdStart, usersVisitInfo.get(profileUser))
        //records == DataRecords
        val (userVisitInfo, records) = aggregator.aggregate(sessionData)
        persistUserVisitInfoBuilder += userVisitInfo
        serverSessionIdStart += sessionData.size
        //将产生的记录写到HDFS中
        writeDataRecords(records, outputBasePath, dataStr, index)

      }
      updateUsersVisitInfo(persistUserVisitInfoBuilder.result())
    }
  }
}
