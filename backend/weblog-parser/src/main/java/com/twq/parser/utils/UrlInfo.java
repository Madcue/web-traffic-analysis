package com.twq.parser.utils;

import static com.twq.parser.utils.ParserUtils.isNullOrEmptyOrDash;
import static com.twq.parser.utils.ParserUtils.notNull;


public class UrlInfo {
    private String rawUrl;
    private String scheme;
    private String hostport;
    private String path;
    private String query;
    private String fragment;

    public UrlInfo(String rawUrl, String scheme, String hostport,
                   String path, String query, String fragment) {
        this.rawUrl = rawUrl;
        this.scheme = scheme;
        this.hostport = hostport;
        this.path = path;
        this.query = query;
        this.fragment = fragment;
    }

    public String getPathQueryFragment() {
        if (isNullOrEmptyOrDash(query) && isNullOrEmptyOrDash(fragment)) {
            return notNull(path);
        } else if (isNullOrEmptyOrDash(query) && !isNullOrEmptyOrDash(fragment)) {
            return path + "#" + fragment;
        } else if (!isNullOrEmptyOrDash(query) && isNullOrEmptyOrDash(fragment)) {
            return path + "?" + query;
        } else {
            return path + "?" + query + "#" + fragment;
        }
    }

    public String getUrlWithoutQuery() {
        if (isNullOrEmptyOrDash(path)) {
            return scheme + "://" + hostport;
        } else {
            return scheme + "://" + hostport + path;
        }
    }

    public String getFullUrl() {
        if (isNullOrEmptyOrDash(rawUrl)) {
            return "-";
        } else {
            return rawUrl;
        }
    }

    public String getDomain() {
        if (isNullOrEmptyOrDash(hostport)) {
            return "-";
        } else {
            return hostport;
        }
    }

    public String getScheme() {
        return scheme;
    }

    public String getPath() {
        return path;
    }

    public String getQuery() {
        return query;
    }

    public String getFragment() {
        return fragment;
    }
}
