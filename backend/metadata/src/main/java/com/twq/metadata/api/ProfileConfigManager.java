package com.twq.metadata.api;

import com.twq.metadata.model.TargetPage;

import java.util.List;

public interface ProfileConfigManager {
    /**
     * 加载所有的目标页面配置
     * @return
     */
    public List<TargetPage> loadAllTargetPagesConfig();
}
