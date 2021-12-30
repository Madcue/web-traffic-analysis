package com.twq.parser.configuration.loader.impl;

import com.twq.metadata.api.ProfileConfigManager;
import com.twq.metadata.model.TargetPage;
import com.twq.parser.configuration.loader.ProfileConfigLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
/**
 *  profile相关配置加载的默认实现
 *  从metadata模块的ProfileConfigManager中加载配置，并且放在内存中
 *  ProfileConfigManager 接口 获取MongoDB中所有目标页面配置信息 并加载
 */
public class DefaultProfileConfigLoader implements ProfileConfigLoader{
     private Map<Integer, List<TargetPage>> profileTargetPages = new HashMap<>();

     public DefaultProfileConfigLoader(ProfileConfigManager profileConfigManager) {
          //类初始化的时候一次性将配置加载，并且放到缓存中

          //加载所有的目标页面配置
          List<TargetPage> allTargetPages = profileConfigManager.loadAllTargetPagesConfig();
          //将所有的目标页面配置按照profileId归类放到Map缓存中，Map的key是profileId，value是对应profileId的所有的目标页面配置
          allTargetPages.forEach(new Consumer<TargetPage>() {
               @Override
               public void accept(TargetPage targetPage) {
                    List<TargetPage> existsTgs = profileTargetPages.get(targetPage.getProfileId());
                    if (existsTgs == null) {//判断是否要创建map
                         List<TargetPage> newTgs = new ArrayList<>();
                         newTgs.add(targetPage);
                         profileTargetPages.put(targetPage.getProfileId(), newTgs);
                    } else {//不需要直接往现有的Map<Integer, List<TargetPage>>加入 key是targetPage.getProfileId() value 对应key映射的值的list集合
                         existsTgs.add(targetPage);
                    }
               }
          });
     }

     @Override
     public Map<Integer, List<TargetPage>> getTargetPages() {
          return profileTargetPages;
     }
}
