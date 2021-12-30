package com.twq.parser.configuration.loader.impl;


import com.twq.parser.configuration.SearchEngineConfig;
import com.twq.parser.configuration.loader.SearchEngineConfigLoader;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * 有一层外部接入(关键词模式匹配方式 样例)
 * 搜索引擎配置的加载的实现类
 * 从配置文件中加载搜索引擎的配置
 */

public class FileSearchEngineConfigLoader implements SearchEngineConfigLoader {

     private List<SearchEngineConfig> searchEngineConfigs = new ArrayList<>();

     public FileSearchEngineConfigLoader() {
          //类构造的时候将所有的搜索引擎配置加载并且解析好，放在内存中
          //读取配置文件searchEngine.conf
          //字节流
          InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("searchEngine.conf");
          //其实就是把数据拉倒SearchEngineConfig实体类保存 在内存中,给提供具体服务类
          try {
               //字节流转字符流
               BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
               //Properties线程安全的多个线程可以共享一个Properties对象，而不需要外部同步
               //load以简单的面向行格式从输入字符流中读取属性properties列表(键和元素对)
               //在此方法返回后，指定的流将保持打开状态。
               //参数:reader—输入的字符流。
               Properties properties = new Properties();
               properties.load(reader);
               properties.forEach(new BiConsumer<Object, Object>() {
                    @Override
                    public void accept(Object key, Object value) {
                         //将配置文件中的每一行配置转换成搜索引擎配置，且将搜索引擎配置放到缓存列表中
                         String strValue = (String) value;
                         String newValue = strValue.replace("\"", "");
                         String[] temps = newValue.split(",");
                         SearchEngineConfig searchEngineConfig = new SearchEngineConfig(Integer.parseInt(key.toString()), stingNull2null(temps[0]),
                                 stingNull2null(temps[1]), stingNull2null(temps[2]));
                         searchEngineConfigs.add(searchEngineConfig);
                    }
               });
               //对搜索引擎配置列表按照keyId进行升序排序
               searchEngineConfigs.sort(new Comparator<SearchEngineConfig>() {
                    @Override
                    public int compare(SearchEngineConfig o1, SearchEngineConfig o2) {
                         return o1.getKeyId() - o2.getKeyId();
                    }
               });
          } catch (UnsupportedEncodingException e) {
               e.printStackTrace();
          } catch (IOException e) {
               e.printStackTrace();
          }

     }

     @Override
     public List<SearchEngineConfig> getSearchEngineConfigs() {
          return searchEngineConfigs;
     }


     public String stingNull2null(String value) {
          if (value == null || "null".equals(value)) {
               return null;
          }
          return value;
     }

}
















