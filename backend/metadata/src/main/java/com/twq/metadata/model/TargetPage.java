package com.twq.metadata.model;

/* BSON sample
{
  "Name": "test",
  "IsDisabled": false,
  "Description": "",
  "Id": "57f8df278a4bf51e2d834be8",
  "ProfileId": 524,
  "MatchPattern": "/haier_regUser3",
  "MatchType": "CONTAINS",
  "MatchWithoutQueryString": false,
  "CreatedDate": "2016-10-02T22:12:46.674+08:00",
  "CreatedBy": "Wd3Migrator",
  "ModifiedDate": "2016-10-02T22:12:46.674+08:00",
  "ModifiedBy": "Wd3Migrator"
}
 */

/**
 *  目标页面配置java bean实体类
 */
public class TargetPage {
     private String id; //id
     private int profileId; //这个目标页面配置所属的profile
     private String name; //目标页面配置名称
     private String description; //描述
     private String matchPattern; //匹配字符串
     private String matchType; //匹配类型
     private boolean matchWithoutQueryString; //匹配时是否忽略url的query参数
     private boolean isEnable; //配置是否开启(有效)

     public String getId() {
          return id;
     }

     public void setId(String id) {
          this.id = id;
     }

     public int getProfileId() {
          return profileId;
     }

     public void setProfileId(int profileId) {
          this.profileId = profileId;
     }

     public String getName() {
          return name;
     }

     public void setName(String name) {
          this.name = name;
     }

     public String getDescription() {
          return description;
     }

     public void setDescription(String description) {
          this.description = description;
     }

     public String getMatchPattern() {
          return matchPattern;
     }

     public void setMatchPattern(String matchPattern) {
          this.matchPattern = matchPattern;
     }

     public String getMatchType() {
          return matchType;
     }

     public void setMatchType(String matchType) {
          this.matchType = matchType;
     }

     public boolean isMatchWithoutQueryString() {
          return matchWithoutQueryString;
     }

     public void setMatchWithoutQueryString(boolean matchWithoutQueryString) {
          this.matchWithoutQueryString = matchWithoutQueryString;
     }

     public boolean isEnable() {
          return isEnable;
     }

     public void setEnable(boolean enable) {
          isEnable = enable;
     }
}

































