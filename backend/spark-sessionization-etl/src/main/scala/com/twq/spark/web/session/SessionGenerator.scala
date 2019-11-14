package com.twq.spark.web.session

import com.twq.parser.dataobject.{BaseDataObject, PvDataObject}

import scala.collection.immutable.VectorBuilder

//每一个会话中含有的数据
//就是一个BaseDataObject列表
case class SessionData(dataObjects: Seq[BaseDataObject])

/**
  * 会话切割逻辑(user级别)
  */
trait SessionGenerator {
  //30分钟
  private[this] val THIRTY_MINS_IN_MS = 1800000L

  /**
    * 对一个user下的所有的访问记录(dataObjects)进行会话的切割，逻辑为：
    * 1、如果两个相邻的访问记录时间相隔超过30分钟，则切割为一个会话
    * 2、如果一个pv是重要的入口，则从这个pv开始重新算作一个新的会话
    *
    * @param sortedObjects 一个user的排序好的所有的dataObjects
    * @return 返回这个user产生的所有的会话，一个user可能产生若干个会话
    */
  def groupDataObjects(sortedObjects: Seq[BaseDataObject]): Seq[SessionData] = {
    // 对sortedObjects的相邻的两个dataObjects进行时间比较，超过30分钟则切成一个新的会话
    val (sessionBuilder, groupBuilder, _) =
      sortedObjects.foldLeft(
        new VectorBuilder[Seq[BaseDataObject]], new VectorBuilder[BaseDataObject], None: Option[BaseDataObject]) {
        //resultBuilder表示会话的列表，groupBuilder表示一个会话中的BaseDataObject列表
        //previous表示前一个BaseDataObject，current表示当前的BaseDataObject
        case ((resultBuilder, groupBuilder, previous), current) =>
          //如果不是第一个BaseDataObject，则用当前的BaseDataObject和前一个BaseDataObject的时间进行比较
          //超过30分钟则切成一个新的会话
          if (previous.isDefined && current.getServerTime.getTime - previous.get.getServerTime.getTime > THIRTY_MINS_IN_MS) {
            resultBuilder += groupBuilder.result()
            groupBuilder.clear()
          }
          //没有超过30分钟，则算在当前的会话中
          groupBuilder += current
          (resultBuilder, groupBuilder, Some(current))
      }
    //如果最后一个会话中含有BaseDataObject的值，则将最后的一个会话加入到会话列表sessionBuilder中
    val lastGroup = groupBuilder.result()
    if (lastGroup.nonEmpty) sessionBuilder += lastGroup

    //根据重要入口再次进行会话的切割
    val sessions = sessionBuilder.result().flatMap { rawSession =>
      //找到重要入口pv的在会话中的index
      //是一个Seq，比如Seq(0, 3, 8, 17)
      val entrances = generateEntranceIndexes(rawSession)
      //Seq(0, 3, 8, 17).zip(Seq(3, 8, 17)) = List((0,3), (3,8), (8,17))
      entrances zip entrances.tail map { case (begin, end) =>
        //进行会话的切割
        rawSession.slice(begin, end)
      }
    }
    //转换成SessionData
    sessions map { dataObjects => SessionData(dataObjects) }
  }

  /**
    *  得到重要入口的index
    * @param rawSession 需要根据重要入口切割会话的会话
    * @return
    */
  private def generateEntranceIndexes(rawSession: Seq[BaseDataObject]): Seq[Int] = {
    @inline def sentAtSameTime(pv1: PvDataObject, pv2: PvDataObject): Boolean =
      pv1 != null && pv2 != null && pv1.getServerTime.getTime == pv2.getServerTime.getTime

    //循环遍历这个会话中的所有的DataObject，找到重要入口的pv
    val entrances = rawSession.zipWithIndex.foldLeft(Seq(0), null: PvDataObject) {
      //entrancesIndexes表示重要入口pv在会话中的index，previous表示前一个pv
      //currentDataObject当前的dataObject，index表示当前的dataObject在会话中的index
      case ((entrancesIndexes, previous), (currentDataObject, index)) =>
        currentDataObject match {
            //如果是pv，且这个pv是一个重要入口，并且这个pv和上一个pv不是同一时间产生的，并且这个pv和前一个pv不一样
            //而且这个pv不是这个会话的第一个dataObject,那么这个pv就是重要入口，就需要从这个pv重新切割会话
          case pv: PvDataObject if pv.isMandatoryEntrance && !sentAtSameTime(pv, previous) && pv.isDifferentFrom(previous) =>
            if (index != 0) (entrancesIndexes ++ Seq(index), pv) else (entrancesIndexes, pv)
          case _ => (entrancesIndexes, previous)
        }
    }._1

    entrances ++ Seq(rawSession.length)
  }
}
