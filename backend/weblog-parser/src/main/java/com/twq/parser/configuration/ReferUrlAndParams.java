package com.twq.parser.configuration;

import java.util.Map;

public class ReferUrlAndParams {
    private String referUrlWithoutQuery;
    private Map<String,String> params;

    public ReferUrlAndParams(String referUrlWithoutQuery,Map<String,String> params){
        this.referUrlWithoutQuery=referUrlWithoutQuery;
        this.params=params;
    }
    public String getReferUrlWithoutQuery() {
        return referUrlWithoutQuery;
    }

    public Map<String, String> getParams() {
        return params;
    }
}
