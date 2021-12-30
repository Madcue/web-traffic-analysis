package com.twq.parser.dataobject.dim;

/**
 * 目标页面配置信息
 */
public class TargetPageInfo {
     private String key;//目标页面的key
     private String targetName;//目标页面名
     private boolean isActive;//是否活跃

     public TargetPageInfo(String key, String targetName, boolean isActive) {
          this.key = key;
          this.targetName = targetName;
          this.isActive = isActive;
     }

     public String getKey() {
          return key;
     }

     public String getTargetName() {
          return targetName;
     }

     public boolean isActive() {
          return isActive;
     }

     public void setKey(String key) {
          this.key = key;
     }

     public void setTargetName(String targetName) {
          this.targetName = targetName;
     }

}
