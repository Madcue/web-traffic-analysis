package com.twq.parser.utils;

import com.twq.parser.utils.ParserUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class UrlParseUtils {
     /**
      * 注意:
      * 这里的解码不仅适用于URL解码,还有其他UTF-8形式的编码也可以通过decode方法解码得到 比如求gstl PageTitle 的字符串为UA%E5%A5%B3%E5%AD%90%E8%BF%90%E5%8A 后面省略
      * 对一个已经编码的字符串进行解码(&gsurl=https%3A%2F%2Fwww.underarmour.cn%2F%3Futm_source%3D...省略)
      * 有些字符串可能已经经过两次编码(比如url的参数等)，所以我们需要二次解码才能真正解码成功，例如：
      * https%3A%2F%2Fwww.underarmour.cn%2F%3Futm_source%3Dbaidu%26utm_term%3D%25E6%25A0%2587%25E9%25A2%2598%26utm_medium%3DBrandZonePC%26utm_channel%3DSEM
      * 第一次解码后为：
      * https://www.underarmour.cn/?utm_source=baidu&utm_term=%E6%A0%87%E9%A2%98&utm_medium=BrandZonePC&utm_channel=SEM
      * 第二次解码后为：
      * https://www.underarmour.cn/?utm_source=baidu&utm_term=标题&utm_medium=BrandZonePC&utm_channel=SEM
      *
      * @param encodedStr 编码后的字符串
      * @return 完全解码后的字符串
      */
     public static String decode(String encodedStr) {
          if (ParserUtils.isNullOrEmptyOrDash(encodedStr)) {
               return encodedStr;
          }
          String decodedStr = "-";
          try {
               decodedStr = decodeTwice(encodedStr);
          } catch (Exception e) {
               e.printStackTrace();
               //有可能url被截断，导致编码后的url不完整，所以decode的报错如下错误：
               // java.lang.IllegalArgumentException: URLDecoder: Incomplete trailing escape (%) pattern
               //比如：https%3A%2F%2Fwww.underarmour.cn%2Fcmens-footwear-running%2F%3Futm_source%3Dbaidu%26utm_campaign%3DPC%2
               //这里采用截断最后一个%后面的字符(包括此%)
               int lastPercentIndex = encodedStr.lastIndexOf("%");
               if (encodedStr.length() - lastPercentIndex == 2) {
                    try {
                         decodedStr = decodeTwice(decodedStr.substring(0, lastPercentIndex));
                    } catch (Exception e1) {
                         e.printStackTrace();
                         return "-";
                    }
               }
          }
          return decodedStr;
     }

     private static String decodeTwice(String str) throws UnsupportedEncodingException {
          //第一次解码
          String decodedStr = URLDecoder.decode(str, "UTF-8");
          //两次解码，因为有一些中文的url参数是两次编码的
          //如果不需要对url进行中文解码,就不做第二次解码,直接返回一次解码的decodedStr
          if (decodedStr.indexOf("%") > 0) {
               decodedStr = URLDecoder.decode(decodedStr, "UTF-8");
          }
          return decodedStr;
     }

     /**
      * 解析字符串的url为UrlInfo
      * 正常规则的url：
      * https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#NewLaunch
      * https://www.underarmour.cn/s-HOVR#NewLaunch?qf=11-149&pf=&sortStr=&nav=640
      *
      * @param url
      * @return UrlInfo
      */
     public static UrlInfo getInfoFromUrl(String url) {
          //去掉字符开始前和结尾后的空格
          String trimedUrl = url.trim();
          if (ParserUtils.isNullOrEmptyOrDash(trimedUrl)) {
               return new UrlInfo("-", "-", "-", "-", "-", "_");
          } else {
               int firstQuestionMarkIndex = trimedUrl.indexOf("?");
               int firstPoundMarkIndex = trimedUrl.indexOf("#");
               try {
                    /**
                     * uri和url的区别：
                     * URI 是统一资源标识符，而 URL 是统一资源定位符。
                     * 关系：
                     * URI 属于 URL 更高层次的抽象，一种字符串文本标准。
                     * 就是说，URI 属于父类，而 URL 属于 URI 的子类。URL 是 URI 的一个子集。
                     */
                    URI uri = new URI(trimedUrl).normalize();//将path标准化
                    int port = uri.getPort();
                    String hostport;//解析到URI的host
                    if (port != -1) {
                         hostport = uri.getHost() + ":" + port;
                    } else {
                         hostport = uri.getHost();
                    }
                    String query; //解析 query,fragment
                    String fragment;
                    if (firstQuestionMarkIndex > 0 && firstQuestionMarkIndex > firstPoundMarkIndex) {
                         query = trimedUrl.substring(firstQuestionMarkIndex + 1);
                         fragment = trimedUrl.substring(firstPoundMarkIndex + 1, firstQuestionMarkIndex);
                    } else {
                         //从标准化的URI,拿到URI的query,query在前 #fragment在后
                         query = uri.getRawQuery();
                         fragment = uri.getRawFragment();
                    }
                    return new UrlInfo(trimedUrl, uri.getScheme(), hostport, uri.getRawPath(), query, fragment);

                    //https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607
               } catch (URISyntaxException e) {//不规则的URI解析逻辑
                    try {
                         if (firstQuestionMarkIndex == -1) {//解析不规则且没有query的url
                              return parseUrlWithoutQuery(trimedUrl, firstPoundMarkIndex, false);
                         } else {//解析不规则但是含有query的url
                              return parseUrlWithQuery(trimedUrl,firstQuestionMarkIndex,firstPoundMarkIndex);
                         }
                    }catch (Exception exception){
                         exception.printStackTrace();
                         return new UrlInfo("-", "-", "-", "-", "-", "-");
                    }

               }
          }
     }

     /**
      * 解析不规则但是含有query的url, 例如：
      * https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607
      * 或者：
      * https://www.underarmour.cn/s-HOVR#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607?qf=11-149&pf=&sortStr=&nav=640
      *
      * @param url
      * @param firstQuestionMarkIndex 第一个 ? 号所在的位置
      * @param firstPoundMarkIndex    第一个 # 号所在的位置
      * @return 获取url的query和fragment
      **/
     private static UrlInfo parseUrlWithQuery(String url, int firstQuestionMarkIndex, int firstPoundMarkIndex) {
          QueryAndFragment queryAndFragment = getQueryAndFragment(url, firstQuestionMarkIndex, firstPoundMarkIndex);
          String urlWithoutQuery = url.substring(0, firstQuestionMarkIndex);
          UrlInfo uriInfo = parseUrlWithoutQuery(urlWithoutQuery,firstPoundMarkIndex,true);
          return new UrlInfo(url, uriInfo.getSchema(), uriInfo.getDomain(), uriInfo.getPath(),////抽离思想,部分调用
                  queryAndFragment.getQuery(),queryAndFragment.getFragment());


     }

     /**
      * 获取url的query和fragment
      * query和fragment的前后顺序不确定， 例如：
      * https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607
      * 或者：
      * https://www.underarmour.cn/s-HOVR#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607?qf=11-149&pf=&sortStr=&nav=640
      *
      * @param url
      * @param firstQuestionMarkIndex 第一个 ? 号所在的位置
      * @param firstPoundMarkIndex    第一个 # 号所在的位置
      * @return
      */

     private static QueryAndFragment getQueryAndFragment(String url, int firstQuestionMarkIndex, int firstPoundMarkIndex) {
          if (firstPoundMarkIndex > 0) {
               if (firstQuestionMarkIndex > firstPoundMarkIndex) {
                    return new QueryAndFragment(url.substring(firstPoundMarkIndex + 1), url.substring(firstPoundMarkIndex + 1, firstQuestionMarkIndex));
               } else {
                    return new QueryAndFragment(url.substring(firstQuestionMarkIndex + 1, firstPoundMarkIndex), url.substring(firstPoundMarkIndex + 1));
               }
          } else {
               return new QueryAndFragment(url.substring(firstQuestionMarkIndex + 1), "");
          }
     }

     /**
      * 解析不规则且没有query的url，例如：
      * https://www.underarmour.cn/cmens-tops-shortsleeve/#11|Mens|Tops|Shortsleeve|2-MensCategory-MensCategory
      *
      * hasQuestionMark = true 的情况,传入的是以下两种
      * https://www.underarmour.cn/s-HOVR#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607
      * https://www.underarmour.cn/s-HOVR
      * @param trimedUrl
      * @param firstPoundMarkIndex 第一个 # 符号的位置
      * @param hasQuestionMark     是否有 ? 标记
      * @return
      */
     private static UrlInfo parseUrlWithoutQuery(String trimedUrl, int firstPoundMarkIndex, boolean hasQuestionMark) {
          //因为不是标准的URI 这里使用自己定义的解码方式
          String decoderUrl = decode(trimedUrl);
          //拿到冒号:的索引
          int colonIndex = decoderUrl.indexOf(":");
          String schema = decoderUrl.substring(0, colonIndex);
          String hostport = decoderUrl.substring(colonIndex + 3, decoderUrl.indexOf("/", colonIndex + 3));
          //更正看绿色字体解释清除了.
          String path = decoderUrl.substring(decoderUrl.indexOf("/", colonIndex + 3));
          //fragment是否需要设置根据情况而定
          String fragment = "-";
          if (firstPoundMarkIndex > 0 && !hasQuestionMark) fragment = trimedUrl.substring(firstPoundMarkIndex + 1);
          return new UrlInfo(trimedUrl, schema, hostport, path, "-", fragment);
     }




     /**
      * 从url中的query中截取每一个参数，例如：
      * query为：qf=11-149&pf=&sortStr=&nav=640
      * 则返回：
      * Map(qf -> 11-149, pf -> "-", sortStr -> "-", nav -> 640)
      *
      * @param query
      * @return 参数键值对
      */
     public static Map<String,String> getQueryParams(String query) {
          Map<String,String> params = new HashMap<>();
          if (ParserUtils.isNullOrEmptyOrDash(query)){
               return params;
          }
          String[] temps = query.split("&");
          for (String str :
                  temps) {
               String[] kvSplit = str.split("=");
               if (kvSplit.length == 2) {
                    params.put(kvSplit[0], kvSplit[1]);
               }else {
                    params.put(kvSplit[0], "-");
               }
          }
          return params;
     }

}