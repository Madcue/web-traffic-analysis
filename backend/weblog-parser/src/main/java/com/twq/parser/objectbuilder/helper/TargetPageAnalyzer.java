package com.twq.parser.objectbuilder.helper;

import com.twq.parser.configuration.TargetConfigMatcher;
import com.twq.parser.configuration.loader.ProfileConfigLoader;
import com.twq.parser.configuration.service.TargetPageConfigService;

import java.util.ArrayList;
import java.util.List;
/**
 *  匹配并得到真正的目标页面的分析类
 *
 *  先要明白一个东西,PvDataObject用于需要充实TargetPageDataObject,且PvDataObject也是构建TargetPageDataObject实体的一部分
 *  拿到MongoDB的配置数据用于
 *  后续和WebLogParser类进行网页的预解析日志做匹配
 *  匹配得到目标页面数据后辅助构建TargetPageDataObject实体
 */

public class TargetPageAnalyzer {

     private TargetPageConfigService targetPageConfigService;
     //持有获取MongoDB中的targetpage数据的接口

     public TargetPageAnalyzer(ProfileConfigLoader profileConfigLoader) {
     this.targetPageConfigService = new TargetPageConfigService(profileConfigLoader.getTargetPages());
     }

     /**
     *  匹配指定的profileId的目标页面
     * @param profileId
     * @param pvUrl
     * @return
     */
     public List<TargetConfigMatcher> getTargetHits(int profileId, String pvUrl) {
          //获取指定profileId对应的所有目标页面匹配类
          List<TargetConfigMatcher> targetConfigMatchers = this.targetPageConfigService.getByProfileId(profileId);

          //将url与所有的目标页面匹配类进行匹配，得到所有可以匹配的目标页面匹配对象
          List<TargetConfigMatcher> targetConfigHits = new ArrayList<>();
          for (TargetConfigMatcher targetConfigMatcher:
              targetConfigMatchers ) {
               if (targetConfigMatcher.match(pvUrl)){
                    targetConfigHits.add(targetConfigMatcher);
               }
          }
          return targetConfigHits;
     }


}
