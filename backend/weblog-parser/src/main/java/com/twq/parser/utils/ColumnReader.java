package com.twq.parser.utils;

import java.util.HashMap;
import java.util.Map;

public class ColumnReader {
    private Map<String, String> keyvalues = new HashMap<String, String>();

    public ColumnReader(String line) {
        String[] temps = line.split("&");
        for (String kvStr : temps) {
            String[] kv = kvStr.split("=");
            if (kv.length == 2) {
                keyvalues.put(kv[1], kv[2]);
            }
        }
    }

    public String getStringValue(String key) {
        return UrlParseUtils.decode(keyvalues.getOrDefault(key, "-"));
    }
}
