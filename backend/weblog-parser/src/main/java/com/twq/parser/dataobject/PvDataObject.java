package com.twq.parser.dataobject;

import com.twq.parser.dataobject.dim.AdInfo;
import com.twq.parser.dataobject.dim.BrowserInfo;
import com.twq.parser.dataobject.dim.ReferrerInfo;
import com.twq.parser.dataobject.dim.SiteResourceInfo;

public class PvDataObject extends BaseDataObject {

    private SiteResourceInfo siteResourceInfo;
    private BrowserInfo browserInfo;
    private ReferrerInfo referrerInfo;
    private AdInfo adInfo;

    private int duration; //表示当前pv停留时长, 精确到秒级别

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    /**
     * 判断当前pv是否是重要的入口
     * @return
     */
    public boolean isMandatoryEntrance() {
        if (referrerInfo.getDomain().equals(siteResourceInfo.getDomain())) {
            return false;
        } else {
            return adInfo.isPaid();
        }
    }

    /**
     *  判断当前的pv是否和相邻的pv相同
     * @param other
     * @return
     */
    public boolean isDifferentFrom(PvDataObject other) {
        if (other == null) {
            return true;
        } else {
            return !referrerInfo.getUrl().equals(other.referrerInfo.getUrl()) ||
                    !siteResourceInfo.getUrl().equals(other.siteResourceInfo.getUrl());
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
