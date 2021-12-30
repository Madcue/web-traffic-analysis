package com.twq.parser.matches;
/**
 *  匹配比较类型
 */
public enum MatchType {
     //这些配置在MongoDB里面,我们与之匹配
     REGEX_MATCH,
     START_WITH,
     END_WITH,
     CONTAINS
}
