package com.twq.parser.dataobject;

import com.twq.parser.dataobject.dim.AdInfo;
import com.twq.parser.dataobject.dim.BrowserInfo;
import com.twq.parser.dataobject.dim.ReferrerInfo;
import com.twq.parser.dataobject.dim.SiteResourceInfo;

public class PvDataObject extends BaseDataObject{
     private SiteResourceInfo siteResourceInfo;
     private BrowserInfo browserInfo;
     private ReferrerInfo referrerInfo;
     private AdInfo adInfo;
     private int duration; //表示当前pv页面停留时长, 精确到秒级别,这个在ETL过程中解析 ClassifiedSessionData调用setDuration UserSessionDataAggregator调用getDuration

     public int getDuration() {
          return duration;
     }

     public void setDuration(int duration) {
          this.duration = duration;
     }

     /**
      * 判断当前pv是否是重要的入口 Mandatory强制的 Entrance入口
      *
      * @return
      */
     public boolean isMandatoryEntrance() {
          if (referrerInfo.getDomain().equals(siteResourceInfo.getDomain())) {
               //如果来源引用信息中的域名和pageView分类信息中的域名相同,则不是强制入口,也就是不是重要入口
               return false;
          } else {
               //否则判断广告信息中是否付费来确定是否是重要入口
               return adInfo.isPaid();
          }
     }

     /**
      * 判断当前的pv是否和相邻的pv相同
      *
      * @param other
      * @return
      */
     public boolean isDifferentFrom(PvDataObject other) {
          if (other == null) {
               return true;
          } else {
               //根据当前PV来源引用信息的url和传入的otherPv来源引用信息的url比较
               //或者当前PV分类信息的url和传入的otherPv分类信息的url比较
               return !referrerInfo.getUrl().equals(other.referrerInfo.getUrl())
                       || !siteResourceInfo.getUrl().equals(other.siteResourceInfo.getUrl());
          }
     }

     public SiteResourceInfo getSiteResourceInfo() {
          return siteResourceInfo;
     }

     public void setSiteResourceInfo(SiteResourceInfo siteResourceInfo) {
          this.siteResourceInfo = siteResourceInfo;
     }

     public BrowserInfo getBrowserInfo() {
          return browserInfo;
     }

     public void setBrowserInfo(BrowserInfo browserInfo) {
          this.browserInfo = browserInfo;
     }

     public ReferrerInfo getReferrerInfo() {
          return referrerInfo;
     }

     public void setReferrerInfo(ReferrerInfo referrerInfo) {
          this.referrerInfo = referrerInfo;
     }

     public AdInfo getAdInfo() {
          return adInfo;
     }

     public void setAdInfo(AdInfo adInfo) {
          this.adInfo = adInfo;
     }
}






























