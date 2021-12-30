package com.twq.parser.utils;

public class ParserUtils {
     //判断一个字符串是否为null empty 或者破折号-
     public static boolean isNullOrEmptyOrDash(String str) {
          if (str == null || str.trim().isEmpty() || str.trim().equals("-") || str.trim().toLowerCase().equals("null")) {
               return true;
          } else {
               return false;
          }
     }

     //判断一个字符串是否不为空,如果为null 空 - 返回 - 否则返回字符串
     public static String notNull(String str) {
          if (isNullOrEmptyOrDash(str)) {
               return "-";
          } else {
               return str;
          }
     }

     //判断一个数字是否等于1
     public static boolean parseBoolean(String number) {
          if ("1".equals(number)) {
               return true;
          } else {
               return false;
          }
     }

}
