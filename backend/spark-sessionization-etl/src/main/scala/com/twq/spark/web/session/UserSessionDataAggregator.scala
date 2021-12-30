package com.twq.spark.web.session

import com.twq.parser.dataobject.dim.TargetPageInfo
import com.twq.parser.dataobject.{EventDataObject, HeartbeatDataObject, McDataObject, PvDataObject, TargetPageDataObject}
import com.twq.spark.web.{AvroRecordBuilder, CombinedId, ConversionBuilder}
import com.twq.spark.web.external.UserVisitInfo
import com.twq.web.{Conversion, Heartbeat, MouseClick, PageView, Session}
import scala.collection.immutable.VectorBuilder

/**
  * 每一个user产生的所有的会话的聚合计算(user级别)
  *
  * @param id                   user唯一标识
  * @param serverSessionIdStart 起始会话id
  */
class UserSessionDataAggregator(
                                 id: CombinedId,
                                 serverSessionIdStart: Long,
                                 lastPersistedUserVisitInfo: Option[UserVisitInfo]
                               ) extends ConversionBuilder with AvroRecordBuilder {

  import com.twq.spark.web.RichCollection._

  /**
    * 会话聚合计算
    * 1、生成最终的Session对象
    * 2、生产最终的Conversion对象
    * 3、生成最终的PageView对象
    * 4、生成最终的Heartbeat对象
    * 5、生成最终的MouseClick对象
    * 调用的AvroRecordBuilder构建者构建完成的DataRecords多个表全维度 将提供给AvroOutputComponent
    *
    * @param sessions 一个user产生的所有的会话
    * @return 这个user产生的所有的聚合后DataRecords
    */
  def aggregate(sessions: Seq[SessionData]): (UserVisitInfo, Seq[DataRecords]) = {
    val classifiedSessionData = sessions.zipWithIndex map { case (sessionData, index) =>
      //给每一个会话中的所有dataObject进行归类
      new ClassifiedSessionData(index, sessionData)
    }

    val initUserVisitInfo = lastPersistedUserVisitInfo getOrElse UserVisitInfo(id, UserVisitInfo.INIT_LAST_VISIT_TIME, 0)
    //对每一个会话进行聚合计算，将最终的实体计算出来
    val (userVisitInfo, finalRecordsBuilder) =
      classifiedSessionData.foldLeft(initUserVisitInfo, Vector.newBuilder[DataRecords]) {
        case ((currentUserVisitInfo, recordsBuilder), data) =>

          //以下所有都是一个[会话级别]的计算
          //计算会话id
          val sessionId = serverSessionIdStart + data.sessionIndex
          //计算这个会话最终的会话实体
          val session = produceSession(sessionId, data, currentUserVisitInfo)
          //计算这个会话中的所有的PageView实体
          val pageViews = producePageViews(data, session)
          //计算这个会话中的所有的Heartbeat实体
          val heartbeats = produceHeartBeats(data.hbDataMap, session)
          //计算这个会话中的所有的MouseClick实体
          val mouseClicks = produceMouseClicks(data.mcData, session, data.pvData)
          //计算这个会话中的转化
          val conversions = produceConversions(sessionId, data.allActiveTargetInfo, data.eventData)
          //将会话的信息派生到转化实体中
          val conversionsWithSession = conversions.map(buildConversion(_, session))
          //更新用户访问信息
          val updatedVisitInfo = currentUserVisitInfo.copy(
            lastVisitIndex = currentUserVisitInfo.lastVisitIndex + 1,
            lastVisitTime = data.sessionStartTime
          )
          //这个会话中的所有的实体对象
          recordsBuilder += DataRecords(session, pageViews, mouseClicks, conversionsWithSession, heartbeats)
          (updatedVisitInfo, recordsBuilder)

      }
    // trait Builder的result()方法,从添加的元素生成一个集合。在此操作之后，构建器的内容是未定义的。return:包含添加到此构建器的元素的集合。
    //trait Builder[-Elem, +To] extends Growable[Elem] def result(): To
    (userVisitInfo, finalRecordsBuilder.result())

  }

  private def produceMouseClicks(mcData: Seq[McDataObject],
                                 session: Session, pvData: Seq[PvDataObject]): Seq[MouseClick] = {
    mcData.map(buildMouseClick(_, session))(scala.collection.breakOut)
  }

  private def produceHeartBeats(hbDataMap: Map[String, HeartbeatDataObject], session: Session): Seq[Heartbeat] = {
    //(scala.collection.breakOut)提供一个CanBuildFrom实例，它构建一个特定的目标集合(To')，而不考虑原始集合(From')
    //val c: Iterable[Heartbeat] = hbDataMap.values.map(buildHeartBeat(_,session))
    //val c: immutable.Seq[Heartbeat] = hbDataMap.values.map(buildHeartBeat(_,session))(scala.collection.breakOut)
    hbDataMap.values.map(buildHeartBeat(_, session))(scala.collection.breakOut)
  }

  private def produceConversions(sessionId: Long,
                                 targetInfoData: Seq[(TargetPageDataObject, TargetPageInfo)],
                                 eventData: Seq[EventDataObject]): Seq[Conversion] = {
    buildConversions(sessionId, targetInfoData, eventData)
  }


  /**
    * 计算Session
    *
    * @param sessionId 会话id
    * @param data      会话中所有的DataObject
    * @return Session(Avro to Java bean的数据填装)
    */
  private def produceSession(sessionId: Long,
                             data: ClassifiedSessionData, userVisitInfo: UserVisitInfo): Session = {
    //Avro to Java bean  Session的Builder方法
    val sessionBuilder: Session.Builder = Session.newBuilder()
    sessionBuilder.setServerSessionId(sessionId)
    //计算会话停留时长
    sessionBuilder.setSessionDuration(data.fetchSessionDuration)

    //计算是否是新的访客
    val isNewVisitor = if (userVisitInfo.lastVisitTime == UserVisitInfo.INIT_LAST_VISIT_TIME) true else false
    //计算这个访客自从上次访问到这次访问中间隔了多少天
    val daysSinceLastVisit =
      if (isNewVisitor) -1
      else {
        import com.github.nscala_time.time.Imports._
        val lastDateMidNight = new DateTime(userVisitInfo.lastVisitTime).toDateMidnight
        val currentDateMidNight = new DateTime(data.sessionStartTime).toDateMidnight
        (lastDateMidNight to currentDateMidNight).toDuration.days.toInt
      }
    //user访问信息维度  这些维度的数据由Avro组件builder writer
    sessionBuilder.setIsNewVisitor(isNewVisitor)
    sessionBuilder.setDaysSinceLastVisit(daysSinceLastVisit)
    sessionBuilder.setUserVisitNumber(userVisitInfo.lastVisitIndex + 1) //访客访问的次数

    //会话特定的页面维度
    val (landingPageViewInfo, secondPageViewInfo, exitPagePageViewInfo) =
      getPageViewInfos(data.pvData, data.selectedPvOpt)
    sessionBuilder.setLandingPageUrl(landingPageViewInfo.url)
    sessionBuilder.setLandingPageOriginalUrl(landingPageViewInfo.originalUrl)
    sessionBuilder.setLandingPageHostname(landingPageViewInfo.hostName)
    sessionBuilder.setLandingPageTitle(landingPageViewInfo.title)

    sessionBuilder.setSecondPageUrl(secondPageViewInfo.url)
    sessionBuilder.setSecondPageOriginalUrl(secondPageViewInfo.originalUrl)
    sessionBuilder.setSecondPageHostname(secondPageViewInfo.hostName)
    sessionBuilder.setSecondPageTitle(secondPageViewInfo.title)

    sessionBuilder.setExitPageUrl(exitPagePageViewInfo.url)
    sessionBuilder.setExitPageOriginalUrl(exitPagePageViewInfo.originalUrl)
    sessionBuilder.setExitPageHostname(exitPagePageViewInfo.hostName)
    sessionBuilder.setExitPageTitle(exitPagePageViewInfo.title)

    //会话实体统计维度
    sessionBuilder.setPvCount(data.pvData.length)
    sessionBuilder.setPvDistinctCount(data.pvData.distinctBy(_.getSiteResourceInfo.getUrl).length)
    sessionBuilder.setIsBounced(data.pvData.size == 1)
    sessionBuilder.setMouseClickCount(data.mcData.length)
    sessionBuilder.setTargetCount(data.allActiveTargetInfo.length)
    sessionBuilder.setEventCount(data.eventData.length)
    sessionBuilder.setConversionCount(data.eventData.length + data.allActiveTargetInfo.length)
    sessionBuilder.setTargetDistinctCount(data.allActiveTargetInfo.distinctBy { case (_, info) => info.getKey }.length)
    sessionBuilder.setEventDistinctCount(data.eventData.distinctBy(e => (e.getEventCategory, e.getEventLabel, e.getEventAction)).length)
    buildSession(data.pvData, data.selectedPvOpt, data.selectedFirstDataObject, sessionBuilder)
  }

  private def getPageViewInfos(pvArray: Seq[PvDataObject], mandatoryPvOpt: Option[PvDataObject]): (PageViewInfo, PageViewInfo, PageViewInfo) =
    pvArray match {
      case Seq(onlyOnePage) => //如果只有一个pv的话，则这个pv是着陆页也是退出页
        val siteResourceInfo = onlyOnePage.getSiteResourceInfo
        val pageInfo = PageViewInfo(siteResourceInfo.getUrl, siteResourceInfo.getOriginalUrl, siteResourceInfo.getDomain, siteResourceInfo.getPageTitle)
        (pageInfo, PageViewInfo.Default, pageInfo)
      case Seq(firstPv, secondPv, _*) => //含有2个pv或者以上的情况
        //着陆页pv先取是重要入口的pv，如果没有重要入口pv的话就取首个pv
        val firstPvSiteResourceInfo = mandatoryPvOpt.getOrElse(firstPv).getSiteResourceInfo
        val secondPvSiteResourceInfo = if (mandatoryPvOpt.nonEmpty && firstPv.isDifferentFrom(mandatoryPvOpt.orNull))
          firstPv.getSiteResourceInfo else secondPv.getSiteResourceInfo
        val lastPv = pvArray.last
        val lastPvSiteResourceInfo =
          if (lastPv.isDifferentFrom(mandatoryPvOpt.orNull))
            firstPv.getSiteResourceInfo else pvArray(pvArray.length - 2).getSiteResourceInfo
        (PageViewInfo(firstPvSiteResourceInfo.getUrl, firstPvSiteResourceInfo.getOriginalUrl, firstPvSiteResourceInfo.getDomain, firstPvSiteResourceInfo.getPageTitle),
          PageViewInfo(secondPvSiteResourceInfo.getUrl, secondPvSiteResourceInfo.getOriginalUrl, secondPvSiteResourceInfo.getDomain, secondPvSiteResourceInfo.getPageTitle),
          PageViewInfo(lastPvSiteResourceInfo.getUrl, lastPvSiteResourceInfo.getOriginalUrl, lastPvSiteResourceInfo.getDomain, lastPvSiteResourceInfo.getPageTitle)
        )

      case _ => (PageViewInfo.Default, PageViewInfo.Default, PageViewInfo.Default)
    }

  /**
    * 计算PageView的字段
    *
    * @param data    当前会话中的所有的dataObject
    * @param session 当前会话
    * @return PageView
    */
  private def producePageViews(data: ClassifiedSessionData, session: Session): Vector[PageView] = {
    val pvData = data.pvData
    val (_, pageViewsBuilder, _) = pvData.zipWithIndex.foldLeft(1, new VectorBuilder[PageView], None: Option[(PvDataObject, Int)]) {
      //pageViewDepth表示页面访问深度，pageViewBuilder表示计算后的PageView的列表
      //previous表示前一个pv及其在所有pv中的index
      //currentPv表示当前pv， currentIndex当前pv在所有pv中的index
      //(op: (B, A) => B): B
      case ((pageViewDepth, pageViewBuilder, previous), (currentPv, currentIndex)) =>
        val recordBuilder = PageView.newBuilder()

        //获取当前pv对应的hb
        val currentPvHbOpt = data.hbDataMap.get(currentPv.getPvId)
        //计算页面加载时长
        val loading = currentPvHbOpt match {
          case Some(hb) => hb.getLoadingDuration
          case None => 0
        }
        recordBuilder.setLoadingDuration(loading)
        recordBuilder.setAccessOrder(currentIndex + 1) //页面的访问顺序,+1因为zipWithIndex
        recordBuilder.setPageDuration(currentPv.getDuration) //页面的停留时长，已经在计算会话停留时长的时候ClassifiedSessionData内方法计算过了
        recordBuilder.setIsExitPage(currentIndex >= pvData.length - 1) //判断是否是退出页

        //计算页面访问深度
        val nextDepth = previous map { case (previousPv, _) =>
          if (currentPv.getSiteResourceInfo.getUrl.equals(previousPv.getSiteResourceInfo.getUrl)) {
            recordBuilder.setIsRefresh(true)
            pageViewDepth
          } else {
            pageViewDepth + 1
          }
        } getOrElse (pageViewDepth)
        recordBuilder.setPageViewDepth(nextDepth)
        //计算PageView的其他的字段
        val filledPageView = buildPageView(currentPv, recordBuilder, session)
        pageViewBuilder += filledPageView
        (nextDepth, pageViewBuilder, Some(currentPv, currentIndex))
    }
    pageViewsBuilder.result()
  }


}


//会话特定的页面维度
private case class PageViewInfo(url: String, originalUrl: String, hostName: String, title: String)

private object PageViewInfo {
  val Default = PageViewInfo("-", "-", "-", "-")
}