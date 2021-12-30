package com.twq.parser.matches;
/**
 * 字符串url的匹配比较器
 * 复用StringMatcher的代码
 */
public class UrlStringMatcher extends StringMatcher {

     private boolean matchWithoutQueryString;

     public UrlStringMatcher(MatchType matchType, String matchPattern, boolean matchWithoutQueryString) {
          super(matchType, matchPattern);
          this.matchWithoutQueryString = matchWithoutQueryString;
     }

     @Override
     public boolean match(String url) {
          if (matchWithoutQueryString) {
               int questionIndex = url.indexOf("?");
               if (questionIndex > 0) {
                    return super.match(url.substring(0, url.indexOf("?") + 1));
               }else {
                    return super.match(url);
               }
          }
          return super.match(url);
     }
}
