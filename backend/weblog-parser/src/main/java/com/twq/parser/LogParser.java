package com.twq.parser;

import com.twq.parser.dataobject.InvalidLogObject;
import com.twq.parser.dataobject.ParsedDataObject;
import com.twq.parser.objectbuilder.AbstractDataObjectBuilder;
import com.twq.prepaser.PreParsedLog;

import java.util.Arrays;
import java.util.List;

/**
 * weblog-parser这个模块对外提供的服务的类
 * 该类中组合了LogParser需要的builders和settings
 */
public class LogParser {
     //需要持有AbstractDataObjectBuilder,拿到具体的PV MC EV HB object,TG目标页面实体需要通过其他方式充实
     //LogParser需要的设置对象
     private LogParserSetting logParserSetting;
     //LogParser中所有的日志builders
     private List<AbstractDataObjectBuilder> builders;

     public LogParser(LogParserSetting logParserSetting, List<AbstractDataObjectBuilder> builders) {
          this.logParserSetting = logParserSetting;
          this.builders = builders;
     }

     /**
      * 日志解析的接口
      * 返回的对象中，含有正常的DataObject，也可能含有无效的DataObject，所以我们返回标识接口ParsedDataObject
      * 不管是正常的DataObject还是无效的DataObject都会实现这个标识接口ParsedDataObject
      *
      * @param preParsedLog
      * @return 返回已经解析好的DataObject
      */
     public List<? extends ParsedDataObject> parse(PreParsedLog preParsedLog) {
          String cmd = preParsedLog.getCommand().toString();//cmd 预解析日志里的 cmd值
          //看看是否是支持的日志类型,通过WebLogParser本地配置的logParserSetting
          if (!logParserSetting.getCmds().contains(cmd)) {
               //无效的网页日志实体
               return Arrays.asList(new InvalidLogObject("not support command"));
          }
          for (AbstractDataObjectBuilder builder :
                  builders) {
               if (builder.getCommand().equals(cmd)) {
                    return builder.doBuildDataObjects(preParsedLog);
               }
          }
          //确定没有找到对应的builder,那么将这个实体作为无效的网页日志实体
          return Arrays.asList(new InvalidLogObject("not support command"));
     }
}