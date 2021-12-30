package com.twq.parser.configuration.loader;

import com.twq.metadata.model.TargetPage;

import java.util.List;
import java.util.Map;

/**
 *  和profile相关配置的加载的接口
 */
public interface ProfileConfigLoader {
     /**
      * 获取所有的目标页面的配置
      * @return
      */
     Map<Integer, List<TargetPage>> getTargetPages();
}
