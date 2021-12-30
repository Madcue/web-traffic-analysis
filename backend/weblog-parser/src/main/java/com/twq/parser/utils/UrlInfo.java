package com.twq.parser.utils;

import com.twq.parser.utils.ParserUtils;
/*网站URL信息
包含:
rawUrl -> https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#NewLaunch
schema -> https
hostport(domain) -> www.underarmour.cn
path ->  /s-HOVR
query -> qf=11-149&pf=&sortStr=&nav=640
fragment -> NewLaunch
 */

public class UrlInfo {
     private String rawUrl;
     private String schema;
     private String hostport;
     private String path;
     private String query;
     private String fragment;
     //set方法提到构造器中,在new UrlInfo初始

     public UrlInfo(String rawUrl, String schema, String hostport,
                    String path, String query, String fragment) {
          this.rawUrl = rawUrl;
          this.schema = schema;
          this.hostport = hostport;
          this.path = path;
          this.query = query;
          this.fragment = fragment;
     }

     //特别提供四个方法 分别获取 getPathQueryFragment,没有query的URL,整个URL(raw),Domain(hostport)
     public String getPathQueryFragment() {
          if (ParserUtils.isNullOrEmptyOrDash(query) && ParserUtils.isNullOrEmptyOrDash(fragment)) {
               return ParserUtils.notNull(path);
          } else if (ParserUtils.isNullOrEmptyOrDash(query) && !ParserUtils.isNullOrEmptyOrDash(fragment)) {
               return path + "#" + fragment;
          } else if (!ParserUtils.isNullOrEmptyOrDash(query) && ParserUtils.isNullOrEmptyOrDash(fragment)) {
               return path + "?" + query;
          } else {
               return path + "?" + query + "#" + fragment;
          }
     }

     public String getUrlWithoutQuery() {
          if (ParserUtils.isNullOrEmptyOrDash(path)) {
               //eg:https://www.underarmour.cn/
               return schema + "://" + hostport;
          } else {
               //eg:https://www.underarmour.cn/s-HOVR
               return schema + "://" + hostport + path;
          }
     }

     //包含业务含义对rawUrl的Set
     public String getFullUrl() {
          if (ParserUtils.isNullOrEmptyOrDash(rawUrl)) {
               return "-";
          } else {
               return rawUrl;
          }
     }

     //包含业务含义对hostport域名的Set
     public String getDomain() {
          if (ParserUtils.isNullOrEmptyOrDash(hostport)) {
               return "-";
          } else {
               return hostport;
          }
     }

     public String getSchema() {
          return schema;
     }

     public String getPath() {
          return path;
     }

     public String getQuery() {
          return query;
     }

     public String getFragment() {
          return fragment;
     }
}




































