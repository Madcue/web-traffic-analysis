package com.twq.parser.objectbuilder.helper;

import com.twq.parser.configuration.ReferUrlAndParams;
import com.twq.parser.configuration.SearchEngineConfig;
import com.twq.parser.configuration.service.SearchEngineConfigService;

import com.twq.parser.dataobject.dim.ReferrerInfo;
import com.twq.parser.utils.UrlParseUtils;

import java.util.Map;

/**
 * 这里是搜索引擎信息处理逻辑的顶层
 * 解析搜索引擎的工具类
 */
public class SearchEngineNameUtil {
     private static SearchEngineConfigService searchEngineConfigService = SearchEngineConfigService.getInstance();

     /**
      * 计算来源url中的搜索引擎和搜索关键词
      *
      * @param referrerInfo
      */
     public static void populateSearchEngineInfoFromRefUrl(ReferrerInfo referrerInfo) {
          //1、匹配搜索引擎配置
          //ReferrerInfo通过UrlParseUtils.getQueryParams解析拿到封装进Map<String, String>的Query
          Map<String, String> referParams = UrlParseUtils.getQueryParams(referrerInfo.getQuery());
          //!ReferUrlAndParams丰富(装载)
          ReferUrlAndParams referUrlAndParams = new ReferUrlAndParams(referrerInfo.getUrlWithoutQuery(), referParams);
          SearchEngineConfig searchEngineConfig = searchEngineConfigService.doMatch(referUrlAndParams);
          //2、设置搜索引擎和关键词
          //丰富ReferrerInfo属性SearchEngineName Keyword eqId
          if (searchEngineConfig != null) {
               referrerInfo.setSearchEngineName(searchEngineConfig.getSearchEngineName());
               //如果配置的搜索引擎的关键词的key不会空的话，则需要从query参数中根据这个key拿到关键词
               if (searchEngineConfig.getSearchKeywordKey() != null) {
                    String keyword = referParams.getOrDefault(searchEngineConfig.getSearchKeywordKey(), "");
                    referrerInfo.setKeyword(keyword);
               }
          } else {
               referrerInfo.setSearchEngineName("-");
               referrerInfo.setKeyword("-");
          }
          //3、设置eqid
          if (referrerInfo.getQuery() != "-" &&
                  referrerInfo.getSearchEngineName().equalsIgnoreCase("baidu")) {
               //eqid不是在PvDataObjectBuilder丰富的 是在此处按业务需求设置
               referrerInfo.setEqId(referParams.getOrDefault("eqid", "-"));
          } else {
               referrerInfo.setEqId("-");
          }
     }


}
