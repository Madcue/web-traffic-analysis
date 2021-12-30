package com.twq.parser.matches;

import java.util.regex.Pattern;

/**
 * 字符串匹配比较器
 */
public class StringMatcher {
     //这些配置我们放在MongoDB里面,在使用的时候给StringMatcher初始化,载入配置的信息,用做传入的String s与之进行match方法
     //匹配类型
     private MatchType matchType;
     //匹配的字符串
     protected String matchPattern;

     public StringMatcher(MatchType matchType, String matchPattern) {
          this.matchType = matchType;
          this.matchPattern = matchPattern;
     }

     //对应的匹配规则
     public boolean match(String s) {
          if (matchType == MatchType.REGEX_MATCH) {
               Pattern pattern = Pattern.compile(matchPattern);
               return pattern.matcher(s).find();
          } else if (matchType == MatchType.START_WITH) {
               return s.startsWith(matchPattern);
          } else if (matchType == MatchType.END_WITH) {
               return s.endsWith(matchPattern);
          } else if (matchType == MatchType.CONTAINS) {
               return s.contains(matchPattern);
          }
          return false;
     }
}
