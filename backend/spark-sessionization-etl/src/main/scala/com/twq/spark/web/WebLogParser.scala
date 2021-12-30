package com.twq.spark.web

import com.twq.metadata.api.impl.MongoProfileConfigManager
import com.twq.parser.{LogParser, LogParserSetting}
import com.twq.parser.configuration.loader.impl.DefaultProfileConfigLoader
import com.twq.parser.dataobject.{BaseDataObject, InvalidLogObject, ParsedDataObject}
import com.twq.parser.objectbuilder.{AbstractDataObjectBuilder, EventDataObjectBuilder, HeartbeatDataObjectBuilder, MouseClickDataObjectBuilder, PvDataObjectBuilder}
import com.twq.parser.objectbuilder.helper.TargetPageAnalyzer
import com.twq.prepaser.PreParsedLog
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

/**
  *  和weblog-parser交互的对象,提供给ETL解析方法
  *  用于处理RDD[PreParsedLog]
  */
object WebLogParser {
  //getLogger方法参数class<?>
  private val logger = LoggerFactory.getLogger(WebLogParser.getClass)
  // HashMap 引入了红黑二叉树设计，当冲突的链表长度大于8时，会将链表转化成红黑二叉树结构
  // JDK1.8  相比 JDK1.7 中的 ConcurrentHashMap， 它抛弃了原有的 Segment 分段锁实现，采用了 CAS乐观锁 + synchronized悲观锁 来保证并发的安全性。
  private val localExceptionCounters = new ConcurrentHashMap[String, Int]
  //静态方法参数Params:
  //nm – property name.属性名
  //val – default value.默认值
  private val LOGGING_THRESHOLD_PER_EXCEPTION = Integer.getInteger("web.logparser.logging.exception.threshold", 5000)//5000条 异常阈值

  //初始化weblog-parser的LogParser对象
  private val parser: LogParser = {
    val cmdSet = new util.HashSet[String]()
    cmdSet.add("pv")
    cmdSet.add("mc")
    cmdSet.add("ev")
    cmdSet.add("hb")
    val logParserSetting =new LogParserSetting(cmdSet)

    val builders = new util.ArrayList[AbstractDataObjectBuilder]()
    val profileConfigManager = new MongoProfileConfigManager()//MongoDB Dao
    val profileConfigServer = new DefaultProfileConfigLoader(profileConfigManager)//load mongoDB-conf
    builders.add(new PvDataObjectBuilder(new TargetPageAnalyzer(profileConfigServer)))//TargetPageAnalyzer构造器实例接口提供getTargetHits方法
    builders.add(new MouseClickDataObjectBuilder)
    builders.add(new EventDataObjectBuilder)
    builders.add(new HeartbeatDataObjectBuilder)

    new LogParser(logParserSetting, builders)
  }
  /**
    * 解析日志
    * @param p 预解析后的日志对象
    * @return 返回一个Seq，类型是(CombinedId, BaseDataObject)二元组
    */
  def parse(p: PreParsedLog) = {

    //Builder[A, Vector[A]] ,Builder构建器允许增量地构建集合。
    /**
      * Type parameters:
        Elem – the type of elements that get added to the builder.添加到构建器中的元素的类型
        To – the type of collection that it produced. 产生的集合的类型
        trait Builder[-Elem, +To] extends Growable[Elem] {
      */
    val dataObjectBuilder:
      mutable.Builder[(CombinedId, BaseDataObject), Vector[(CombinedId, BaseDataObject)]]
    = Vector.newBuilder[(CombinedId, BaseDataObject)]
    //LogParser.parse进行doBuildDataObjects
    val parsedObjects: util.List[_ <: ParsedDataObject] = parser.parse(p)
    import scala.collection.JavaConversions.asScalaBuffer
    parsedObjects foreach {
          //关键点mutable Builder可变的,dataObjectBuilder组织的 vector不可变的immutable序列集合使用IndexedSeq实现
      case base: BaseDataObject =>
        val userId = base.getUserId
        dataObjectBuilder += CombinedId(base.getProfileId, userId) -> base
      case invalid: InvalidLogObject =>
        tryLogException("Invalid data object while parsing RequestInfo\n, details: ", new RuntimeException(invalid.getEvent))
    }
    //result of Vector[(CombinedId, BaseDataObject)]
    dataObjectBuilder.result()

  }

  /**
    *  记录异常日志信息
    *  因为数据量比较大，所以异常信息可能会比较多，所以呢对于每一种异常只记录一定量的日志信息
    *  超过这个量的话则不记录了
    * @param errorMsg 错误信息
    * @param ex 异常
    */
  private def tryLogException(errorMsg: String, ex: Exception): Unit = {
    val exceptionName = ex.getClass.getSimpleName
    val current = Option(localExceptionCounters.get(exceptionName)).getOrElse(0)
    localExceptionCounters.put(exceptionName, current + 1)
    if (current < LOGGING_THRESHOLD_PER_EXCEPTION) {
      logger.error(errorMsg, ex)
    }
  }
}
