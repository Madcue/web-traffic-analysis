package com.twq.parser.utils;

import java.util.HashMap;
import java.util.Map;

public class DateUtils {
    private static Map<Integer, String> weekStrMap = new HashMap<>();

    static {
        weekStrMap.put(1, "星期日");
        weekStrMap.put(2, "星期一");
        weekStrMap.put(3, "星期二");
        weekStrMap.put(4, "星期三");
        weekStrMap.put(5, "星期四");
        weekStrMap.put(6, "星期五");
        weekStrMap.put(7, "星期六");
    }

    public static String getChineseWeekStr(Integer field) {
        return weekStrMap.get(field);
    }
}
