package com.twq.spark.web.external

import com.twq.spark.web.CombinedId

/**
  * 用户访问信息获取检索与更新功能
  */
trait UserVisitInfoComponent {
  /**
    * 根据访客唯一标识查询访客的历史访问信息
    * case class CombinedId(profileId: Int, userId: String)
    * case class UserVisitInfo(CombinedId lastVisitTime lastVisitIndex)
    *
    * @param ids
    * @return
    */
  def retrieveUsersVisitInfo(ids: Seq[CombinedId]): Map[CombinedId, UserVisitInfo]

  /**
    * 更新访客的历史访问信息
    *
    * @param users
    */
  def updateUsersVisitInfo(users: Seq[UserVisitInfo]): Unit

}
