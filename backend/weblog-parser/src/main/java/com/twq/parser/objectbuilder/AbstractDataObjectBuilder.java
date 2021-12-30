package com.twq.parser.objectbuilder;

import com.twq.iplocation.IpLocationParser;
import com.twq.parser.dataobject.BaseDataObject;
import com.twq.parser.utils.ColumnReader;
import com.twq.prepaser.PreParsedLog;
import eu.bitwalker.useragentutils.UserAgent;

import java.util.List;

public abstract class AbstractDataObjectBuilder {
     public abstract String getCommand();

     public abstract List<BaseDataObject> doBuildDataObjects(PreParsedLog preParsedLog);

     //AbstractDataObjectBuilder填充公共字段方法
     public void fillCommonBaseDataObjectValue(BaseDataObject baseDataObject,
                                               PreParsedLog preParsedLog, ColumnReader columnReader) {
          baseDataObject.setProfileId(preParsedLog.getProfileId());//GWD=000123 这个网站标识000123
          baseDataObject.setServerTimeString(preParsedLog.getServerTime().toString());

          baseDataObject.setUserId(columnReader.getStringValue("gsuid"));
          baseDataObject.setTrackerVersion(columnReader.getStringValue("gsver"));
          baseDataObject.setPvId(columnReader.getStringValue("pgid"));
          baseDataObject.setCommand(columnReader.getStringValue("gscmd"));

          //结合ip位置信息
          baseDataObject.setClientIp(preParsedLog.getClientIp().toString());
          baseDataObject.setIpLocation(IpLocationParser.parse(preParsedLog.getClientIp().toString()));
          //解析UserAgent信息
          baseDataObject.setUserAgent(preParsedLog.getUserAgent().toString());
          //eu.bitwalker.useragentutils 工具包
          baseDataObject.setUserAgentInfo(UserAgent.parseUserAgentString(preParsedLog.getUserAgent().toString()));

     }

}


