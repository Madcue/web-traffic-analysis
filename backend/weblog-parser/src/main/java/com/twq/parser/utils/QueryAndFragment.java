package com.twq.parser.utils;

//解析网站url中的query 和 剩余片段Fragment
//比如预解析网页日志,通过UrlParseUtils UTF-8格式解码为以下形式
//https://www.bing.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#NewLaunch
//则 Query 为?后的 qf=11-149&pf=&sortStr=&nav=640 Fragment为 #后的 NewLaunch
public class QueryAndFragment {
     private String query;
     private String fragment;

     public QueryAndFragment(String query, String fragment) {
          this.query = query;
          this.fragment = fragment;
     }

     public String getQuery() {
          return query;
     }

     public String getFragment() {
          return fragment;
     }
}
