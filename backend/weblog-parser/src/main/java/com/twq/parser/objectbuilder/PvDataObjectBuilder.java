package com.twq.parser.objectbuilder;

import com.twq.parser.configuration.TargetConfigMatcher;
import com.twq.parser.dataobject.BaseDataObject;
import com.twq.parser.dataobject.PvDataObject;
import com.twq.parser.dataobject.TargetPageDataObject;
import com.twq.parser.dataobject.dim.*;
import com.twq.parser.objectbuilder.helper.SearchEngineNameUtil;
import com.twq.parser.objectbuilder.helper.TargetPageAnalyzer;
import com.twq.parser.utils.ColumnReader;
import com.twq.parser.utils.ParserUtils;
import com.twq.parser.utils.UrlInfo;
import com.twq.parser.utils.UrlParseUtils;
import com.twq.prepaser.PreParsedLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PvDataObjectBuilder extends AbstractDataObjectBuilder {

    private TargetPageAnalyzer targetPageAnalyzer;

    public PvDataObjectBuilder(TargetPageAnalyzer targetPageAnalyzer) {
        this.targetPageAnalyzer = targetPageAnalyzer;
    }

    @Override
    public String getCommand() {
        return "pv";
    }

    @Override
    public List<BaseDataObject> doBuildDataObjects(PreParsedLog preParsedLog) {
        List<BaseDataObject> dataObjects = new ArrayList<>();
        PvDataObject pvDataObject = new PvDataObject();
        ColumnReader columnReader = new ColumnReader(preParsedLog.getQueryString());
        fillCommonBaseDataObjectValue(pvDataObject, preParsedLog, columnReader);

        //1、网站信息的解析
        pvDataObject.setSiteResourceInfo(createSiteResourceInfo(columnReader));
        //2、广告信息的解析
        pvDataObject.setAdInfo(createAdInfo(pvDataObject.getSiteResourceInfo()));
        //3、浏览器信息的解析
        pvDataObject.setBrowserInfo(createBrowserInfo(columnReader));
        //4、来源信息的解析
        pvDataObject.setReferrerInfo(createReferrerInfo(columnReader));

        //6、来源类型和来源渠道的解析
        resolveReferrerDerivedColumns(pvDataObject.getReferrerInfo(), pvDataObject.getAdInfo());

        dataObjects.add(pvDataObject);

        //7、目标页面数据对象的解析
        TargetPageDataObject targetPageDataObject = populateTargetPageObject(preParsedLog, pvDataObject, columnReader);
        if (targetPageDataObject != null) {
            dataObjects.add(targetPageDataObject);
        }

        return dataObjects;
    }

    /**
     *  计算当前的pv对应的目标页面
     * @param preParsedLog
     * @param pvDataObject
     * @param columnReader
     * @return
     */
    private TargetPageDataObject populateTargetPageObject(PreParsedLog preParsedLog,
                                                          PvDataObject pvDataObject, ColumnReader columnReader) {
        //得到指定的profileId所有已经匹配到的目标页面匹配对象
        List<TargetConfigMatcher> targetConfigMatchers =
                targetPageAnalyzer.getTargetHits(preParsedLog.getProfileId(), pvDataObject.getSiteResourceInfo().getUrl());
        if (targetConfigMatchers != null && !targetConfigMatchers.isEmpty()) {
            //将目标页面匹配对象转换成有业务含义的目标页面信息，并且构建TargetPageDataObject
            List<TargetPageInfo> targetPageInfos =
                    targetConfigMatchers.stream()
                            .map(tm -> new TargetPageInfo(tm.getKey(), tm.getTargetName(), tm.isActive()))
                            .collect(Collectors.toList());
            TargetPageDataObject targetPageDataObject = new TargetPageDataObject();
            fillCommonBaseDataObjectValue(targetPageDataObject, preParsedLog, columnReader);

            targetPageDataObject.setTargetPageInfos(targetPageInfos);
            targetPageDataObject.setPvDataObject(pvDataObject);
            return targetPageDataObject;
        }
        return null;
    }

    private void resolveReferrerDerivedColumns(ReferrerInfo referrerInfo, AdInfo adInfo) {
        //来源渠道的计算逻辑：
        //1、先赋值为广告系列渠道，如果没有的话则赋值为搜索引擎，如果还没有的话则赋值为来源域名
        String adChannel = adInfo.getUtmChannel();
        if (!ParserUtils.isNullOrEmptyOrDash(adChannel)) {
            referrerInfo.setChannel(adChannel);
        } else if (!ParserUtils.isNullOrEmptyOrDash(referrerInfo.getSearchEngineName())) {
            referrerInfo.setChannel(referrerInfo.getSearchEngineName());
        } else {
            referrerInfo.setChannel(referrerInfo.getDomain());
        }

        //来源类型计算逻辑
        if (!ParserUtils.isNullOrEmptyOrDash(referrerInfo.getSearchEngineName())) {
            if (adInfo.isPaid()) {
                //从搜索引擎中过来且是付费流量
                referrerInfo.setReferType("paid search"); //付费搜索
            } else {
                //从搜索引擎中过来但不是付费流量
                referrerInfo.setReferType("organic search"); //自然搜索
            }
        } else if (!ParserUtils.isNullOrEmptyOrDash(referrerInfo.getDomain())) {
            //从非搜索引擎的网站中过来
            referrerInfo.setReferType("referral"); //引荐，其实就是外部链接
        } else {
            //直接访问
            referrerInfo.setReferType("direct"); //直接访问
        }
    }

    private SiteResourceInfo createSiteResourceInfo(ColumnReader columnReader) {
        SiteResourceInfo siteResourceInfo = new SiteResourceInfo();
        siteResourceInfo.setPageTitle(columnReader.getStringValue("gstl"));
        String gsurl = columnReader.getStringValue("gsurl");
        UrlInfo urlInfo = UrlParseUtils.getInfoFromUrl(gsurl);
        siteResourceInfo.setUrl(urlInfo.getFullUrl());
        siteResourceInfo.setDomain(urlInfo.getDomain());
        siteResourceInfo.setQuery(urlInfo.getQuery());
        siteResourceInfo.setOriginalUrl(columnReader.getStringValue("gsorurl"));
        return siteResourceInfo;
    }

    private AdInfo createAdInfo(SiteResourceInfo siteResourceInfo) {
        Map<String, String> landingParams = UrlParseUtils.getQueryParams(siteResourceInfo.getQuery());
        AdInfo adInfo = new AdInfo();
        adInfo.setUtmCampaign(landingParams.getOrDefault("utm_campaign", "-"));
        adInfo.setUtmMedium(landingParams.getOrDefault("utm_medium", "-"));
        adInfo.setUtmContent(landingParams.getOrDefault("utm_content", "-"));
        adInfo.setUtmChannel(landingParams.getOrDefault("utm_channel", "-"));
        adInfo.setUtmTerm(landingParams.getOrDefault("utm_term", "-"));
        adInfo.setUtmSource(landingParams.getOrDefault("utm_source", "-"));
        adInfo.setUtmAdGroup(landingParams.getOrDefault("utm_adgroup", "-"));
        return adInfo;
    }

    private BrowserInfo createBrowserInfo(ColumnReader columnReader) {
        BrowserInfo browserInfo = new BrowserInfo();
        browserInfo.setAlexaToolBar(ParserUtils.parseBoolean(columnReader.getStringValue("gsalexaver")));
        browserInfo.setBrowserLanguage(columnReader.getStringValue("gsbrlang"));
        browserInfo.setColorDepth(columnReader.getStringValue("gsclr"));
        browserInfo.setCookieEnable(ParserUtils.parseBoolean(columnReader.getStringValue("gsce")));
        browserInfo.setDeviceName(columnReader.getStringValue("dvn"));
        browserInfo.setDeviceType(columnReader.getStringValue("dvt"));
        browserInfo.setFlashVersion(columnReader.getStringValue("gsflver"));
        browserInfo.setJavaEnable(ParserUtils.parseBoolean(columnReader.getStringValue("gsje")));
        browserInfo.setOsLanguage(columnReader.getStringValue("gsoslang"));
        browserInfo.setResolution(columnReader.getStringValue("gsscr"));
        browserInfo.setSilverlightVersion(columnReader.getStringValue("gssil"));
        return browserInfo;
    }

    private ReferrerInfo createReferrerInfo(ColumnReader columnReader) {
        ReferrerInfo referrerInfo = new ReferrerInfo();
        String referUrl = columnReader.getStringValue("gsref");
        if (referUrl == "-") {
            referrerInfo.setChannel("-");
            referrerInfo.setDomain("-");
            referrerInfo.setEqId("-");
            referrerInfo.setSearchEngineName("-");
            referrerInfo.setUrl("-");
            referrerInfo.setQuery("-");
            referrerInfo.setUrlWithoutQuery("-");
            referrerInfo.setKeyword("-");
        } else {
            UrlInfo urlInfo = UrlParseUtils.getInfoFromUrl(referUrl);
            referrerInfo.setDomain(urlInfo.getDomain());
            referrerInfo.setUrl(urlInfo.getFullUrl());
            referrerInfo.setQuery(urlInfo.getQuery());
            referrerInfo.setUrlWithoutQuery(urlInfo.getUrlWithoutQuery());

            //5、搜索引擎和关键词的解析
            SearchEngineNameUtil.populateSearchEngineInfoFromRefUrl(referrerInfo);
        }
        return referrerInfo;
    }

}
