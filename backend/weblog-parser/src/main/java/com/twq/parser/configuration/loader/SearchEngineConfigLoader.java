package com.twq.parser.configuration.loader;


import com.twq.parser.configuration.SearchEngineConfig;

import java.util.List;

public interface SearchEngineConfigLoader {
    /**
     * 获取所有的搜索引擎配置信息
     * @return
     */
    List<SearchEngineConfig> getSearchEngineConfigs();
}
