package com.twq.spark.web

import com.twq.parser.dataobject.{BaseDataObject, HeartbeatDataObject, McDataObject, PvDataObject}
import com.twq.web.{Conversion, PageView, Session, Heartbeat, MouseClick}
import eu.bitwalker.useragentutils.DeviceType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.collection.immutable.HashMap

object AvroRecordBuilder {
  //所有实体的类名
  val SESSION = classOf[Session].getSimpleName
  val CONVERSION = classOf[Conversion].getSimpleName
  val HEARTBEAT = classOf[Heartbeat].getSimpleName
  val MOUSE_CLICK = classOf[MouseClick].getSimpleName
  val PAGE_VIEW = classOf[PageView].getSimpleName

  //所有实体对应的Schema
  val SCHEMAS = HashMap[String, Schema](
    CONVERSION -> Conversion.getClassSchema,
    HEARTBEAT -> Heartbeat.getClassSchema,
    MOUSE_CLICK -> MouseClick.getClassSchema,
    PAGE_VIEW -> PageView.getClassSchema,
    SESSION -> Session.getClassSchema)
}

trait AvroRecordBuilder {

  import com.twq.parser.utils.ParserUtils._

  def buildSession(pvs: Seq[PvDataObject],
                   selectedPVOpt: Option[PvDataObject],
                   selectedFirstDataObject: BaseDataObject,
                   sessionBuilder: Session.Builder): Session = {
    //通用字段
    sessionBuilder.setTrackerVersion(notNull(selectedFirstDataObject.getTrackerVersion))
    sessionBuilder.setProfileId(selectedFirstDataObject.getProfileId)
    sessionBuilder.setUserId(selectedFirstDataObject.getUserId)

    //时间维度
    sessionBuilder.setSessionServerTime(notNull(selectedFirstDataObject.getServerTimeString))
    sessionBuilder.setHourOfDay(selectedFirstDataObject.getHourOfDay)
    sessionBuilder.setMonthOfYear(selectedFirstDataObject.getMonthOfYear)
    sessionBuilder.setWeekOfYear(selectedFirstDataObject.getWeekOfYear)
    sessionBuilder.setDayOfWeek(selectedFirstDataObject.getDayOfWeek)

    //位置维度
    sessionBuilder.setCountry(notNull(selectedFirstDataObject.getIpLocation.getCountry))
    sessionBuilder.setProvince(notNull(selectedFirstDataObject.getIpLocation.getRegion))
    sessionBuilder.setCity(notNull(selectedFirstDataObject.getIpLocation.getCity))
    val longitude = selectedFirstDataObject.getIpLocation.getLongitude
    sessionBuilder.setLongitude(if (isNullOrEmptyOrDash(longitude)) longitude.toFloat else 0F)
    val latitude = selectedFirstDataObject.getIpLocation.getLatitude
    sessionBuilder.setLatitude(if (isNullOrEmptyOrDash(latitude)) longitude.toFloat else 0F)
    sessionBuilder.setClientIp(selectedFirstDataObject.getClientIp)

    val headPV = pvs.head

    val pv = selectedPVOpt getOrElse headPV

    //来源维度
    sessionBuilder.setReferrerUrl(pv.getReferrerInfo.getUrl)
    sessionBuilder.setReferrerHostname(notNull(pv.getReferrerInfo.getDomain).toLowerCase)
    sessionBuilder.setSourceType(notNull(pv.getReferrerInfo.getReferType))
    sessionBuilder.setChannelName(notNull(pv.getReferrerInfo.getChannel))
    sessionBuilder.setSearchEngine(notNull(pv.getReferrerInfo.getSearchEngineName))
    sessionBuilder.setKeywords(notNull(pv.getReferrerInfo.getKeyword))
    sessionBuilder.setKeywordId(notNull(pv.getReferrerInfo.getEqId))

    //广告维度
    val adInfo = pv.getAdInfo
    sessionBuilder.setAdCampaign(notNull(adInfo.getUtmCampaign))
    sessionBuilder.setAdChannel(notNull(adInfo.getUtmChannel))
    sessionBuilder.setAdSource(notNull(adInfo.getUtmSource))
    sessionBuilder.setAdMedium(notNull(adInfo.getUtmMedium))
    sessionBuilder.setAdKeywords(notNull(adInfo.getUtmTerm))
    sessionBuilder.setAdContent(notNull(adInfo.getUtmContent))
    sessionBuilder.setIsPaidTraffic(adInfo.isPaid)

    //系统以及浏览器维度
    sessionBuilder.setUserAgent(notNull(selectedFirstDataObject.getUserAgent))
    sessionBuilder.setBrowserBrief(notNull(selectedFirstDataObject.getUserAgentInfo.getBrowser.getName))
    sessionBuilder.setDeviceBrand(notNull(selectedFirstDataObject.getUserAgentInfo.getOperatingSystem.getManufacturer.getName))
    sessionBuilder.setIsMobile(selectedFirstDataObject.getUserAgentInfo.getOperatingSystem.getDeviceType == DeviceType.MOBILE)
    sessionBuilder.setDeviceType(notNull(selectedFirstDataObject.getUserAgentInfo.getOperatingSystem.getDeviceType.getName))
    sessionBuilder.setDeviceName(selectedFirstDataObject.getUserAgentInfo.getOperatingSystem.getName)
    sessionBuilder.setScreenResolution(notNull(pv.getBrowserInfo.getResolution))
    sessionBuilder.setColorDepth(notNull(pv.getBrowserInfo.getColorDepth))
    sessionBuilder.setFlashVersion(notNull(pv.getBrowserInfo.getFlashVersion))
    sessionBuilder.setSilverlightVersion(notNull(pv.getBrowserInfo.getSilverlightVersion))
    sessionBuilder.setJavaEnabled(pv.getBrowserInfo.isJavaEnable)
    sessionBuilder.setCookieEnabled(pv.getBrowserInfo.isCookieEnable)
    sessionBuilder.setAlexaToolbarInstalled(pv.getBrowserInfo.isAlexaToolBar)
    sessionBuilder.setOsLanguage(notNull(pv.getBrowserInfo.getOsLanguage))
    sessionBuilder.setBrowserLanguage(notNull(pv.getBrowserInfo.getBrowserLanguage))
    //如果pv中已经含有设备类型和设备名称的话，则优先选择
    if (!isNullOrEmptyOrDash(pv.getBrowserInfo.getDeviceType)) {
      sessionBuilder.setDeviceType(notNull(pv.getBrowserInfo.getDeviceType))
    }
    if (!isNullOrEmptyOrDash(pv.getBrowserInfo.getDeviceName)) {
      sessionBuilder.setDeviceName(notNull(pv.getBrowserInfo.getDeviceName))
    }

    sessionBuilder.build()
  }

  def buildPageView(pv: PvDataObject, recordBuilder: PageView.Builder, session: Session): PageView = {
    recordBuilder.setPageViewId(pv.getPvId)
    recordBuilder.setPageViewServerTime(pv.getServerTimeString)
    recordBuilder.setPageUrl(pv.getSiteResourceInfo.getUrl)
    recordBuilder.setPageOriginalUrl(pv.getSiteResourceInfo.getOriginalUrl)
    recordBuilder.setPageHostname(pv.getSiteResourceInfo.getDomain)
    recordBuilder.setPageTitle(pv.getSiteResourceInfo.getPageTitle)
    recordBuilder.setPageViewReferrerUrl(pv.getReferrerInfo.getUrl)
    val pageView = recordBuilder.build()
    fillBase(pageView, pv) //基本字段的填充
    fillTimeRelated(pageView, pv) //时间相关字段的填充
    fillSession(pageView, session) //派生会话字段
    pageView
  }

  def buildHeartbeat(hb: HeartbeatDataObject, session: Session): Heartbeat = {
    val record = Heartbeat.newBuilder().build()
    fillBase(record, hb)
    record.setServerSessionId(session.getServerSessionId)
    record.setServerTime(notNull(hb.getServerTimeString))
    record.setLoadingDuration(hb.getLoadingDuration)
    record.setPageViewId(hb.getPvId)
    record
  }

  def buildMouseClick(mc: McDataObject, session: Session): MouseClick = {
    val record = MouseClick.newBuilder().build()
    fillBase(record, mc)
    fillTimeRelated(record, mc)
    fillSession(record, session)
    record.setPageViewId(notNull(mc.getPvId))
    record.setMouseClickServerTime(notNull(mc.getServerTimeString))
    record.setClickPageUrl(notNull(mc.getUrl))
    record.setClickPageHostname(notNull(mc.getPageHostName).toLowerCase)
    record.setClickPageTitle(notNull(mc.getPageTitle))

    record.setClickX(mc.getClickX)
    record.setClickY(mc.getClickY)
    record.setPageVersion(notNull(mc.getPageVersion))
    record.setRegionId(mc.getPageRegion)
    record.setSnapshotId(mc.getSnapshotId)
    if(isNullOrEmptyOrDash(mc.getClickScreenResolution))
      record.setClickScreenResolution(session.getScreenResolution)
    else
      record.setClickScreenResolution(mc.getClickScreenResolution)

    record.setLinkHostname(notNull(mc.getLinkHostName).toLowerCase)
    record.setLinkUrl(notNull(mc.getLinkUrl))
    record.setLinkText(notNull(mc.getLinkText))
    record.setLinkX(mc.getLinkX)
    record.setLinkY(mc.getLinkY)
    record.setLinkHeight(mc.getLinkHeight)
    record.setLinkWidth(mc.getLinkWidth)
    record.setIsLinkClicked(mc.isLinkClicked)
    record
  }

  def buildConversion(conversion: Conversion, session: Session): Conversion = {
    conversion.setTrackerVersion(session.getTrackerVersion)
    conversion.setProfileId(session.getProfileId)
    conversion.setUserId(session.getUserId)

    fillSession(conversion, session)
    conversion
  }

  private def fillBase(record: GenericRecord, base: BaseDataObject): Unit = {
    record.put("tracker_version", base.getTrackerVersion)
    record.put("profile_id", base.getProfileId)
    record.put("user_id", notNull(base.getUserId))
  }

  private def fillTimeRelated(record: GenericRecord, base: BaseDataObject): Unit = {
    record.put("hour_of_day", base.getHourOfDay)
    record.put("month_of_year", base.getMonthOfYear)
    record.put("day_of_week", base.getDayOfWeek)
    record.put("week_of_year", base.getWeekOfYear)
  }

  private def fillSession(record: GenericRecord, session: Session) {
    record.put("days_since_last_visit", session.getDaysSinceLastVisit)
    record.put("user_visit_number", session.getUserVisitNumber)
    record.put("is_new_visitor", session.getIsNewVisitor)

    record.put("server_session_id", session.getServerSessionId)
    record.put("session_duration", session.getSessionDuration)
    record.put("session_server_time", session.getSessionServerTime)

    record.put("referrer_url", session.getReferrerUrl)
    record.put("referrer_hostname", session.getReferrerHostname)
    record.put("source_type", session.getSourceType)
    record.put("channel_name", session.getChannelName)
    record.put("search_engine", session.getSearchEngine)
    record.put("keywords", session.getKeywords)
    record.put("keyword_id", session.getKeywordId)

    record.put("ad_campaign", session.getAdCampaign)
    record.put("ad_channel", session.getAdChannel)
    record.put("ad_source", session.getAdSource)
    record.put("ad_medium", session.getAdMedium)
    record.put("ad_keywords", session.getAdKeywords)
    record.put("ad_content", session.getAdContent)
    record.put("is_paid_traffic", session.getIsPaidTraffic)

    record.put("user_agent", session.getUserAgent)
    record.put("browser_brief", session.getBrowserBrief)
    record.put("browser_detail", session.getBrowserDetail)
    record.put("is_mobile", session.getIsMobile)
    record.put("browser_language", session.getBrowserLanguage)
    record.put("device_brand", session.getDeviceBrand)
    record.put("device_type", session.getDeviceType)
    record.put("device_name", session.getDeviceName)
    record.put("screen_resolution", session.getScreenResolution)
    record.put("color_depth", session.getColorDepth)
    record.put("flash_version", session.getFlashVersion)
    record.put("silverlight_version", session.getSilverlightVersion)
    record.put("java_enabled", session.getJavaEnabled)
    record.put("cookie_enabled", session.getCookieEnabled)
    record.put("alexa_toolbar_installed", session.getAlexaToolbarInstalled)
    record.put("os_language", session.getOsLanguage)

    record.put("country", session.getCountry)
    record.put("province", session.getProvince)
    record.put("city", session.getCity)
    record.put("longitude", session.getLongitude)
    record.put("latitude", session.getLatitude)
    record.put("client_ip", session.getClientIp)

    record.put("landing_page_url", session.getLandingPageUrl)
    record.put("landing_page_original_url", session.getLandingPageOriginalUrl)
    record.put("landing_page_hostname", session.getLandingPageTitle)
    record.put("landing_page_title", session.getLandingPageTitle)
    record.put("second_page_url", session.getSecondPageUrl)
    record.put("second_page_original_url", session.getSecondPageOriginalUrl)
    record.put("second_page_hostname", session.getSecondPageHostname)
    record.put("second_page_title", session.getSecondPageTitle)
    record.put("exit_page_url", session.getExitPageUrl)
    record.put("exit_page_original_url", session.getExitPageOriginalUrl)
    record.put("exit_page_hostname", session.getExitPageHostname)
    record.put("exit_page_title", session.getExitPageTitle)

    record.put("is_bounced", session.getIsBounced)
    record.put("pv_count", session.getPvCount)
    record.put("pv_distinct_count", session.getPvDistinctCount)
    record.put("conversion_count", session.getConversionCount)
    record.put("event_count", session.getEventCount)
    record.put("event_distinct_count", session.getEventDistinctCount)
    record.put("target_count", session.getTargetCount)
    record.put("target_distinct_count", session.getTargetDistinctCount)
    record.put("mouse_click_count", session.getMouseClickCount)
  }
}
