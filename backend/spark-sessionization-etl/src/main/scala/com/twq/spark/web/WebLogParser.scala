package com.twq.spark.web

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.twq.metadata.api.impl.MongoProfileConfigManager
import com.twq.parser.configuration.loader.impl.DefaultProfileConfigLoader
import com.twq.parser.dataobject.{BaseDataObject, InvalidLogObject}
import com.twq.parser.{LogParser, LogParserSetting}
import com.twq.parser.objectbuilder._
import com.twq.parser.objectbuilder.helper.TargetPageAnalyzer
import com.twq.prepaser.PreParsedLog
import org.slf4j.LoggerFactory

/**
  *  和weblog-parser交互的对象
  */
object WebLogParser {
  private val logger = LoggerFactory.getLogger(WebLogParser.getClass)
  private val localExceptionCounters = new ConcurrentHashMap[String, Int]
  private val LOGGING_THRESHOLD_PER_EXCEPTION = Integer.getInteger("web.logparser.logging.exception.threshold", 5000)

  //初始化weblog-parser的LogParser对象
  private val parser = {
    val cmdSet = new util.HashSet[String]()
    cmdSet.add("pv")
    cmdSet.add("mc")
    cmdSet.add("ev")
    cmdSet.add("hb")
    val logParserSetting = new LogParserSetting(cmdSet)

    val builders = new util.ArrayList[AbstractDataObjectBuilder]()
    val profileConfigManager = new MongoProfileConfigManager()
    val profileConfigService = new DefaultProfileConfigLoader(profileConfigManager);
    builders.add(new PvDataObjectBuilder(new TargetPageAnalyzer(profileConfigService)))
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
  def parse(p: PreParsedLog): Seq[(CombinedId, BaseDataObject)] = {
    val dataObjectBuilder = Vector.newBuilder[(CombinedId, BaseDataObject)]

    val parsedObjects = parser.parse(p)
    import scala.collection.JavaConversions.asScalaBuffer
    parsedObjects foreach {
      case base: BaseDataObject =>
        val userId = base.getUserId
        dataObjectBuilder += CombinedId(base.getProfileId, userId) -> base
      case invalid: InvalidLogObject =>
        tryLogException("Invalid data object while parsing RequestInfo\n, details: ", new RuntimeException(invalid.getEvent))
    }
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
    if (current < LOGGING_THRESHOLD_PER_EXCEPTION)
      logger.error(errorMsg, ex)
  }
}