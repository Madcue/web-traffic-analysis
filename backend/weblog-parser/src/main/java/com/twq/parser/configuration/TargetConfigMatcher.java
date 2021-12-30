package com.twq.parser.configuration;

import com.twq.parser.matches.UrlStringMatcher;

//这是匹配到的结果保存的地方,是pv(预处理日志)与 MongoDB拉取的数据相匹配 如果能匹配则存储在这个数据存储底层中
public class TargetConfigMatcher {
     private String key; //tagetId
     private String targetName; //name
     private boolean isActive; //isActive
     //匹配规则
     private UrlStringMatcher urlStringMatcher;

     public TargetConfigMatcher(String key, String targetName, boolean isActive, UrlStringMatcher urlStringMatcher) {
          this.key = key;
          this.targetName = targetName;
          this.isActive = isActive;
          this.urlStringMatcher = urlStringMatcher;
     }

     public String getKey() {
          return key;
     }

     public void setKey(String key) {
          this.key = key;
     }

     public boolean isActive() {
          return isActive;
     }

     public String getTargetName() {
          return targetName;
     }

     public void setTargetName(String targetName) {
          this.targetName = targetName;
     }

     /**
     *  匹配一个url是否是当前的目标页面
     * @param url
     * @return
     */
     public boolean match(String url) {
          return urlStringMatcher.match(url);
     }
}
