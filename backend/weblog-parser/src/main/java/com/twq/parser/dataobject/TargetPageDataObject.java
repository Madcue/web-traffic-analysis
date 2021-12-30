package com.twq.parser.dataobject;

import com.twq.parser.dataobject.dim.TargetPageInfo;

import java.util.List;

/**
 * 目标页面实体
 */
public class TargetPageDataObject extends BaseDataObject {
     private List<TargetPageInfo> targetPageInfos;//持有多个目标页面信息
     private PvDataObject pvDataObject;//持有pageView的实体用于充实目标页面实体
     //也就是说 目标页面实体不仅包含目标页面信息还包含pageView的实体


     public List<TargetPageInfo> getTargetPageInfos() {
          return targetPageInfos;
     }

     public void setTargetPageInfos(List<TargetPageInfo> targetPageInfos) {
          this.targetPageInfos = targetPageInfos;
     }

     public PvDataObject getPvDataObject() {
          return pvDataObject;
     }

     public void setPvDataObject(PvDataObject pvDataObject) {
          this.pvDataObject = pvDataObject;
     }
}
