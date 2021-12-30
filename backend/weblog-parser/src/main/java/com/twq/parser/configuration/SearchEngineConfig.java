package com.twq.parser.configuration;

import com.twq.parser.matches.MatchType;
import com.twq.parser.matches.StringMatcher;

public class SearchEngineConfig {
     /** 这里是搜索引擎配置信息的实体 是最底层的数据存储实体 它的上层是通过实现SearchEngineConfigLoader接口的,这里额外说明 虽然它是实体存放数据,
      * 但是在比较的时候会将本身的数据传给中间层StringMatcher,用StringMatcher去跟传入的数据(预解析日志)进行比较
      * FileSearchEngineConfigLoader ,而SearchEngineConfigService 持有了 FileSearchEngineConfigLoader,是它的上层
      * 但真正提供功能的是 SearchEngineNameUtil ,SearchEngineNameUtil持有了 SearchEngineConfigService
      * 具体的使用它doMatch方法,doMatch方法需要的参数是一个ReferUrlAndParams类型的实体,
      * ReferUrlAndParams类型的实体 是 ReferrerInfo实体提供两个参数(referrerInfo.getUrlWithoutQuery(), referParams)来充实的
      * 1.ReferUrlAndParams referUrlAndParams = new ReferUrlAndParams(referrerInfo.getUrlWithoutQuery(), referParams);
      * SearchEngineNameUtil的实际调用者是PvDataObjectBuilder,它调用SearchEngineNameUtil提供的populateSearchEngineInfoFromRefUrl方法
      * 这个populateSearchEngineInfoFromRefUrl方法里面包含1.这段代码.
      * 记住,真正充实它需要的ReferrerInfo,这个ReferrerInfo是由 PvDataObjectBuilder充实的,
      *  是在执行pvDataObject.setReferrerInfo(createReferrerInfo(columnReader));方法的时候,
      *  所以说 PvDataObjectBuilder是最上层的构建者,最终WebLogParser 对象创建 new PvDataObjectBuilder,
      *  WebETL对 PreparseETL进行解析的日志存在Hive表中的数据进行 WebETL,WebETL才会在更上层,它对HDFS中的Hive表数据流调用WebLogParser.parse(p)
      *  那么粗略总结:程序需要做预解析顶层ETL和会话切割业务顶层相关的ETL,也就是PreparseETL 和 WebETL
      *
      * 根据来源url和来源url中的参数和当前的这个搜索引擎配置进行匹配
      * 判断出来源是否是属于当前这个搜索引擎
      *
      * @param referUrlAndParams
      * @return 如果属于当前的搜索引擎则返回true, 否则返回false
      */
     private int keyId;
     private String searchEngineName;
     private String regexUrl;
     private String searchKeywordKey;

     private StringMatcher stringMatcher;//将本地配置载入到stringMatcher

     public SearchEngineConfig(int keyId, String searchEngineName, String regexUrl, String searchKeywordKey) {
          this.keyId = keyId;
          this.searchEngineName = searchEngineName;
          this.regexUrl = regexUrl;
          this.searchKeywordKey = searchKeywordKey;
          //采用正则匹配,匹配来源url和搜索引擎配置的正则url
          //stringMatcher比较器是一个中间层 将两方数据进行比较
          this.stringMatcher = new StringMatcher(MatchType.REGEX_MATCH, this.regexUrl);
     }

     public int getKeyId() {
          return keyId;
     }

     public String getSearchEngineName() {
          return searchEngineName;
     }

     public String getSearchKeywordKey() {
          return searchKeywordKey;
     }
     /**
      *  根据来源url和来源url中的参数和当前的这个搜索引擎配置进行匹配
      *  判断出来源是否是属于当前这个搜索引擎
      * @param referUrlAndParams
      * @return 如果属于当前的搜索引擎则返回true,否则返回false
      */
     public boolean match(ReferUrlAndParams referUrlAndParams) {
          //来源url和当前搜索引擎的regexUrl进行正则匹配
          if (stringMatcher.match(referUrlAndParams.getReferUrlWithoutQuery())) {
               //如果当前搜索引擎的关键词key不为空的话，则需要看看来源url中的query参数中是否含有这个key
               if (searchKeywordKey != null) {
                    return referUrlAndParams.getParams().containsKey(searchKeywordKey);
               } else {
                    return true;
               }
          }
          return false;
     }

}
