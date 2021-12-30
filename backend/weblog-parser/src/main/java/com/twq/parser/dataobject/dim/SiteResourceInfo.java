package com.twq.parser.dataobject.dim;

/**
 * 当前pv网页信息类 ,page view持有SiteResourceInfo
 */
public class SiteResourceInfo {
     private String url;
     private String originalUrl;//原生url
     private String pageTitle;//页面标题
     private String domain;//旧事的host,通过DNS(Domain Name System)映射IP(host) 和 域名(Domain)
     private String query;

     public String getUrl() {
          return url;
     }

     public void setUrl(String url) {
          this.url = url;
     }

     public String getOriginalUrl() {
          return originalUrl;
     }

     public void setOriginalUrl(String originalUrl) {
          this.originalUrl = originalUrl;
     }

     public String getPageTitle() {
          return pageTitle;
     }

     public void setPageTitle(String pageTitle) {
          this.pageTitle = pageTitle;
     }

     public String getDomain() {
          return domain;
     }

     public void setDomain(String domain) {
          this.domain = domain;
     }

     public String getQuery() {
          return query;
     }

     public void setQuery(String query) {
          this.query = query;
     }
}
