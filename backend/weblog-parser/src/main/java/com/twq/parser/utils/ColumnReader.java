package com.twq.parser.utils;

import java.util.HashMap;
import java.util.Map;


/**
 * 按照一定的规则解析query_string，将所有的kv值放到内存中
 * 便于根据key获取相对应的value
 */
public class ColumnReader {
     private Map<String, String> keyvalues = new HashMap<>();

     public ColumnReader(String line) {
          //eg:&pvid=291355119hvjhz11
          String[] temps = line.split("&");
          for (String kvStr :
                  temps) {
               String[] kv = kvStr.split("=");
               if (kv.length == 2) {
                    keyvalues.put(kv[0], kv[1]);
               }
          }
     }

     public String getStringValue(String key) {
          //通过Java的URLDecoder对UTF-8字符编码的value(传入的key所对应的)字符串解码,否则返回默认值"-"
          return UrlParseUtils.decode(keyvalues.getOrDefault(key,"-"));
     }
}
