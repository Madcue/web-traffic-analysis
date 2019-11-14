package com.twq.spark.web.session

import com.twq.parser.dataobject._
import com.twq.parser.dataobject.dim.TargetPageInfo

/**
  * 给一个会话中的所以dataObject进行归类(会话级别)
  * @param sessionIndex 表示是user的第几个会话
  * @param session  一个会话中所有的dataObject
  */
class ClassifiedSessionData(
  val sessionIndex : Int,
  session: SessionData ) {

  val (pvData, hbDataMap, targetPageData, mcData, eventData) = {
    val pvBdr = Vector.newBuilder[PvDataObject]
    val hbMapBdr = Map.newBuilder[String, HeartbeatDataObject]
    val targetInfoBdr = Vector.newBuilder[TargetPageDataObject]
    val mcBdr = Vector.newBuilder[McDataObject]
    val eventBdr = Vector.newBuilder[EventDataObject]

    //对一个会话中的所有的dataObject进行归类
    session.dataObjects foreach { dataObject =>
      dataObject match {
        case pv: PvDataObject =>
          pvBdr += pv
        case hb: HeartbeatDataObject =>
          hbMapBdr += hb.getPvId -> hb
        case tp: TargetPageDataObject =>
          targetInfoBdr += tp
        case mc: McDataObject =>
          mcBdr += mc
        case event: EventDataObject =>
          eventBdr += event
      }
    }
    (pvBdr.result(), hbMapBdr.result(), targetInfoBdr.result(), mcBdr.result(), eventBdr.result())
  }

  protected val objects: Seq[BaseDataObject] = session.dataObjects

  //这个会话中的第一个pv的发生的时间就是会话开始的时间
  //如果这个会话中没有pv的话，那么就取第一个dataObject发生的时间
  val sessionStartTime = (pvData.headOption getOrElse objects.head).getServerTime.getTime

  //当前会话选中的pv，即profileId大于0且是重要入口的pv
  def selectedPVOpt: Option[PvDataObject] = pvData.find(pv => pv.isMandatoryEntrance && pv.getProfileId > 0)
  //当前会话中的第一个dataObject
  def firstDataObject: BaseDataObject = objects.find(_.getProfileId > 0).getOrElse(objects.head)
  //选择第一个dataObject
  //先选择重要入口的pv，没有的话则取第一个dataObject
  def selectedFirstDataObject: BaseDataObject = selectedPVOpt.getOrElse(firstDataObject)

  /**
    *  当前会话中所有的有效的目标页面
    * @return
    */
  val allActiveTargetInfo: Seq[(TargetPageDataObject, TargetPageInfo)] = {
    import scala.collection.JavaConversions.asScalaBuffer
    targetPageData.flatMap(target => target.getTargetPageInfos.collect { case info if info.isActive => (target, info) })
  }

  /**
    * 计算会话时长
    *  会话时长等于这个会话中所有的pv的停留时间
    * @return 会话时长
    */
  def fetchSessionDuration: Int = {
    if (objects.isEmpty) 0
    else {
      //设置pv的停留时长
      setPVDuration(pvData.toList, true)
      //返回所有的pv的停留时间之和
      pvData.map(_.getDuration).sum
    }
  }

  /**
    *  设置pv停留时长
    * @param pvs  所有的pv
    * @param isFirstPv 是否是第一个pv
    * @return
    */
  private def setPVDuration(pvs: List[PvDataObject], isFirstPv: Boolean = false): Any = {
    pvs match {
      case first :: second :: rest => { //当前的一个pv的停留时间等于后一个pv发生的时间减去当前pv发生的时间
        val pvDuration = ((second.getServerTime.getTime - first.getServerTime.getTime) / 1000).toInt
        first.setDuration(pvDuration)
        //递归计算后续且非最后一个pv的停留时间
        setPVDuration(second :: rest)
      }
      case last :: Nil => //最后一个pv的停留时间的计算
        val currentPvHbOpt = hbDataMap.get(last.getPvId) //获取这个pv对应的hb
        val pvDuration = currentPvHbOpt match {
            //如果对应的hb存在的话则取hb中的页面停留时间
          case Some(hb) if (hb.getClientPageDuration != 0) => hb.getClientPageDuration
          case _ =>
            //计算整个会话中的pv的平均停留时间
            val averagePvDuration = ((last.getServerTime.getTime - sessionStartTime) / Math.max(pvData.size - 1, 1)) / 1000
            //如果平均的pv的停留时间小于0的话，则为0，否则就取平均停留时间
            val defaultLastPvDuration = if (averagePvDuration < 0L) 0 else averagePvDuration.toInt
            //计算最后一个dataObject发生的时间和最后一个pv发生的时间的间隔
            val lastObjectTime = objects.last.getServerTime.getTime
            val calculated = ((lastObjectTime - last.getServerTime.getTime) / 1000).toInt
            //如果calculated小于0则去平均停留时间，否则就取calculated
            if (calculated < 0) defaultLastPvDuration else calculated
        }
        last.setDuration(pvDuration)
    }
  }
}
