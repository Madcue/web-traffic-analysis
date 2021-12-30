package com.twq.parser.configuration.service;

import com.twq.metadata.model.TargetPage;
import com.twq.parser.configuration.TargetConfigMatcher;
import com.twq.parser.matches.MatchType;
import com.twq.parser.matches.UrlStringMatcher;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 和profile相关的目标页面配置的服务类,为TargetPageAnalyzer服务的类,最终目的是充实TargetConfigMatcher 里面有所有MongoDB里存的目标页面信息
 */
public class TargetPageConfigService {

     private Map<Integer, List<TargetPage>> targetPageMap = null;

     public TargetPageConfigService(Map<Integer, List<TargetPage>> targetPageMap) {
          //这里通过TargetPageConfigService构造器 给到DefaultProfileConfigLoader的Map==profileTargetPages
          //真正传参的是通过TargetPageAnalyzer 持有TargetPageConfigService, 给它的构造器分配接口
          //而TargetPageAnalyzer被PvDataObjectBuilder 和 WebLogParser 调用
          //具体的传参给接口的操作是在WebLogParser
          //它具体做的事情是
          //构建一个    val builders = new util.ArrayList[AbstractDataObjectBuilder]()
          //接口的实现   val profileConfigManager = new MongoProfileConfigManager()
          //    通过    val profileConfigService = new DefaultProfileConfigLoader(profileConfigManager);
          //这样profileConfigService就拿到了Loader,Loader还需要给TargetPageConfigService构造器,
          //真正的使用者是PvDataObjectBuilder
          //          builders.add(new PvDataObjectBuilder(new TargetPageAnalyzer(profileConfigService)))
          //具体使用的是TargetConfigMatcher 这里装载了MongoDB中targetPage配置数据,将它转化为TargetPageInfo供PvDataObjectBuilder使用
          //PvDataObjectBuilder会将 pvDataObject(SiteResourceInfo,BrowserInfo,ReferrerInfo,AdInfo),TargetPageDataObject 实体充实好
          this.targetPageMap = targetPageMap;
     }
     /**
      *  获取指定profileId的所有的目标页面的配置
      *  并且将每一个目标页面配置转化成可以匹配的实体
      * @param profileId
      * @return
      */
     //TargetConfigMatcher 字符串匹配类
     public List<TargetConfigMatcher> getByProfileId(int profileId) {
          //获取指定profileId相对应的所有目标页面配置
          List<TargetPage> targetPages = targetPageMap.getOrDefault(profileId, Collections.<TargetPage>emptyList());
          //将目标页面配置转换成目标页面匹配类
          List<TargetConfigMatcher> targetConfigMatchers = targetPages.stream().map(targetPage -> {
               return new TargetConfigMatcher(targetPage.getId(), targetPage.getName(), targetPage.isEnable(),
                       new UrlStringMatcher(MatchType.valueOf(targetPage.getMatchType()),
                               targetPage.getMatchPattern(), targetPage.isMatchWithoutQueryString()));
          }).collect(Collectors.toList());
          return targetConfigMatchers;//返回匹配上的目标页面
     }
}

