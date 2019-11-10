package com.twq.parser.matches;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class StringMatcherTest {

    @Test
    public void match() {
        StringMatcher stringMatcher = new StringMatcher(MatchType.REGEX_MATCH, "(\\w+\\.)*baidu\\.(com|cn|com\\.cn).*");
        Assert.assertTrue(stringMatcher.match("https://www.baidu.com/baidu.php"));

        stringMatcher = new StringMatcher(MatchType.CONTAINS, "/baidu.php");
        Assert.assertTrue(stringMatcher.match("https://www.baidu.com/baidu.php?test=2"));

        stringMatcher = new StringMatcher(MatchType.END_WITH, "/baidu.php");
        Assert.assertTrue(stringMatcher.match("https://www.baidu.com/baidu.php"));

        stringMatcher = new StringMatcher(MatchType.START_WITH, "https://");
        Assert.assertTrue(stringMatcher.match("https://www.baidu.com/baidu.php"));
    }
}