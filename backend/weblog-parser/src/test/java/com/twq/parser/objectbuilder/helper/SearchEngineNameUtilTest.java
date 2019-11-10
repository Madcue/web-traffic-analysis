package com.twq.parser.objectbuilder.helper;

import com.twq.parser.dataobject.dim.ReferrerInfo;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SearchEngineNameUtilTest {

    @Test
    public void populateSearchEngineInfoFromRefUrl() {
        ReferrerInfo referrerInfo = new ReferrerInfo();
        referrerInfo.setUrlWithoutQuery("https://www.baidu.com/baidu.php");
        referrerInfo.setQuery("wd=运动鞋品牌排行&eqid=ead342b60003ccd3000000035b3b492a");

        SearchEngineNameUtil.populateSearchEngineInfoFromRefUrl(referrerInfo);

        Assert.assertEquals("baidu", referrerInfo.getSearchEngineName());
        Assert.assertEquals("运动鞋品牌排行", referrerInfo.getKeyword());
        Assert.assertEquals("ead342b60003ccd3000000035b3b492a", referrerInfo.getEqId());
    }
}