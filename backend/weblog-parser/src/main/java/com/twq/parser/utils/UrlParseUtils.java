package com.twq.parser.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import static com.twq.parser.utils.ParserUtils.isNullOrEmptyOrDash;


public class UrlParseUtils {
    public static String decode(String encodedStr) {
        if (ParserUtils.isNullOrEmptyOrDash(encodedStr)) {
            return encodedStr;
        }
        String decodedStr = "-";
        try {
            decodedStr = decodeTwice(encodedStr);
        } catch (Exception e) {
            e.printStackTrace();
            int lastPercentIndex = encodedStr.lastIndexOf("%");
            if (encodedStr.length() - lastPercentIndex == 2) {
                try {
                    decodedStr = decodeTwice(encodedStr.substring(0, lastPercentIndex));
                } catch (Exception e1) {
                    e1.printStackTrace();
                    return "-";
                }
            }
        }
        return decodedStr;
    }

    private static String decodeTwice(String str) throws Exception {
        String decodedStr = URLDecoder.decode(str, "utf-8");
        //twice decode
        if (decodedStr.indexOf("%") > 0) {
            decodedStr = URLDecoder.decode(decodedStr, "utf-8");
        }
        return decodedStr;
    }

    public static UrlInfo getInfoFromUrl(String url) {
        String trimedUrl = url.trim();
        if (isNullOrEmptyOrDash(trimedUrl)) {
            return new UrlInfo("-", "-", "-", "-", "-", "-");
        } else {
            int firstQuestionMarkIndex = trimedUrl.indexOf("?");
            int firstPoundMarkIndex = trimedUrl.indexOf("#");
            try {
                URI uri = new URI(trimedUrl).normalize();
                int port = uri.getPort();
                String hostport;
                if (port != -1) {
                    hostport = uri.getHost() + ":" + port;
                } else {
                    hostport = uri.getHost();
                }
                String query;
                String fragment;
                if (firstPoundMarkIndex > 0 && firstQuestionMarkIndex > firstPoundMarkIndex) {
                    query = trimedUrl.substring(firstQuestionMarkIndex + 1);
                    fragment = trimedUrl.substring(firstPoundMarkIndex + 1, firstQuestionMarkIndex);
                } else {
                    query = uri.getRawQuery();
                    fragment = uri.getRawFragment();
                }
                return new UrlInfo(trimedUrl, uri.getScheme(), hostport, uri.getRawPath(), query, fragment);
            } catch (URISyntaxException e) {
                try {
                    if (firstQuestionMarkIndex == -1) {
                        return parseUrlWithoutQuery(trimedUrl, firstPoundMarkIndex, false);
                    } else {
                        return parseUrlWithQuery(trimedUrl, firstQuestionMarkIndex, firstPoundMarkIndex);
                    }
                } catch (Exception exception) {
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
     * @return
     */
    private static UrlInfo parseUrlWithQuery(String url, int firstQuestionMarkIndex, int firstPoundMarkIndex) {
        QueryAndFragment queryAndFragment = getQueryAndFragment(url, firstQuestionMarkIndex, firstPoundMarkIndex);
        String urlWithoutQuery = url.substring(0, firstQuestionMarkIndex);
        UrlInfo uriInfo = parseUrlWithoutQuery(urlWithoutQuery, firstPoundMarkIndex, true);
        return new UrlInfo(url, uriInfo.getScheme(), uriInfo.getDomain(),
                uriInfo.getPath(), queryAndFragment.getQuery(), queryAndFragment.getFragment());
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
    private static QueryAndFragment getQueryAndFragment(String url,
                                                        int firstQuestionMarkIndex, int firstPoundMarkIndex) {
        if (firstPoundMarkIndex > 0) {
            if (firstQuestionMarkIndex > firstPoundMarkIndex) {
                return new QueryAndFragment(url.substring(firstQuestionMarkIndex + 1), url.substring(firstPoundMarkIndex + 1, firstQuestionMarkIndex));
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
     * @param trimedUrl
     * @param firstPoundMarkIndex 第一个 # 符号的位置
     * @param hasQuestionMark     是否有 ? 标记
     * @return
     */
    private static UrlInfo parseUrlWithoutQuery(String trimedUrl, int firstPoundMarkIndex, boolean hasQuestionMark) {
        String decoderUrl = decode(trimedUrl);
        int colonIndex = decoderUrl.indexOf(":");
        String scheme = decoderUrl.substring(0, colonIndex);
        String hostport = decoderUrl.substring(colonIndex + 3, decoderUrl.indexOf("/", colonIndex + 3));
        String path = decoderUrl.substring(decoderUrl.indexOf("/", colonIndex + 3));
        String fragment = "-";
        if (firstPoundMarkIndex > 0 && !hasQuestionMark) fragment = trimedUrl.substring(firstPoundMarkIndex + 1);
        return new UrlInfo(trimedUrl, scheme, hostport, path, "-", fragment);
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
    public static Map<String, String> getQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (ParserUtils.isNullOrEmptyOrDash(query)) {
            return params;
        }
        String[] temps = query.split("&");
        for (String str : temps) {
            String[] kv = str.split("=");
            if (kv.length == 2) {
                params.put(kv[0], kv[1]);
            } else if (kv.length == 1) {
                params.put(kv[0], "-");
            }
        }
        return params;
    }
}