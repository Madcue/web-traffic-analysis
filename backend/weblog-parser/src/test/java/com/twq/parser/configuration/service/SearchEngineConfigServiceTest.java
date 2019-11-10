package com.twq.parser.configuration.service;

import com.twq.parser.configuration.ReferUrlAndParams;
import com.twq.parser.configuration.SearchEngineConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class SearchEngineConfigServiceTest {

    private SearchEngineConfigService searchEngineConfigService = SearchEngineConfigService.getInstance();

    @Test
    public void doMatch() {
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("wd", "运动鞋品牌排行");
        ReferUrlAndParams referUrlAndParams = new ReferUrlAndParams("https://www.baidu.com/baidu.php", paramsMap);
        SearchEngineConfig searchEngineConfig = searchEngineConfigService.doMatch(referUrlAndParams);

        Assert.assertNotNull(searchEngineConfig);
        Assert.assertEquals("baidu", searchEngineConfig.getSearchEngineName());
        Assert.assertEquals("wd", searchEngineConfig.getSearchKeywordKey());
    }
}