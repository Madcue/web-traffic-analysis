package com.twq.parser.utils;

public class ParserUtils {
    public static boolean isNullOrEmptyOrDash(String str) {
        return str == null || str.trim().isEmpty() ||
                str.trim().equals("-") || str.trim().toLowerCase().equals("null");

    }

    public static String notNull(String str) {
        if (isNullOrEmptyOrDash(str)) {
            return "-";
        } else {
            return str;
        }
    }

    public static boolean parseBoolean(String number) {
        if ("1".equals(number)) {
            return true;
        } else {
            return false;
        }
    }
}
