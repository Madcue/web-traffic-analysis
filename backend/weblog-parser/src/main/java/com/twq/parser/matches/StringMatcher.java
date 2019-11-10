package com.twq.parser.matches;

import java.util.regex.Pattern;

/**
 *  字符串匹配比较器
 */
public class StringMatcher {
    private MatchType matchType;
    private String matchPattern;

    public StringMatcher(MatchType matchType,String matchPattern){
        this.matchType=matchType;
        this.matchPattern=matchPattern;
    }
    public boolean match(String s){
        if(matchType==MatchType.REGEX_MATCH){
            Pattern pattern = Pattern.compile(matchPattern);
            return pattern.matcher(s).find();
        }else if (matchType == MatchType.START_WITH){
            return s.startsWith(matchPattern);
        }else if (matchType == MatchType.END_WITH){
            return s.endsWith(matchPattern);
        }else if(matchType==MatchType.CONTAINS){
            return s.contains(matchPattern);
        }
        return false;
    }
}
