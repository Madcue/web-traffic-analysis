package com.twq.spark.web.session

import com.twq.web.{Conversion, PageView, Session, Heartbeat, MouseClick}

/**
  *  一个会话中的所有的最终实体，和我们的表结构一一对应
  * @param sessionRecord  会话实体
  * @param pageViewRecords  会话中所有的PageView实体
  * @param mouseClickRecords 会话中所有的MouseClick实体
  * @param conversionRecords 会话中所有的Conversion实体
  * @param heartbeats 会话中所有的Heartbeat实体
  */
case class DataRecords (
                       sessionRecord: Session,
                       pageViewRecords: Seq[PageView],
                       mouseClickRecords: Seq[MouseClick],
                       conversionRecords: Seq[Conversion],
                       heartbeats: Seq[Heartbeat]
                       )
