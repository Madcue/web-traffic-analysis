package com.twq.spark.web

object CombinedId {
  // Both the 'profile id' and 'user id' are either 'numeric' or 'alphanumeric', separator with one char is enough
  private val SEPARATOR = "@"

  /**
    * 解码访问了某一个网站的访客的唯一标识
    * @param str 编码的访问了某一个网站的访客的唯一标识
    * @return
    */
  def decode(str: String): CombinedId = {
    // The input 'str' is expected to be "${lastDecimalBitOfUserIdHash}${profileId}${SEPARATOR}${userId}"
    val parts = str.split(SEPARATOR)
    assert(parts.size == 2, s"Invalid string ${str}, couldn't be decoded to CombinedId")
    CombinedId(parts(0).tail.toInt, parts(1))
  }
}

/**
  *  唯一标识访问了某一个网站的访客
  * @param profileId 访客访问的网站的唯一标识
  * @param userId 访客的唯一标识
  */
case class CombinedId(profileId: Int, userId: String) {
  import CombinedId._

  /**
    *  编码访问了某一个网站的访客的唯一标识
    * @return
    */
  def encode: String ={
    // the encoding scheme:
    // "${lastDecimalBitOfUserIdHash}${profileId}${SEPARATOR}${userId}"
    s"${userId.hashCode.toString.last}${profileId}${SEPARATOR}${userId}"
  }
}
