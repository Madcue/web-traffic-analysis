package com.twq.parser.dataobject;

import com.twq.parser.dataobject.dim.TargetPageInfo;

import java.util.List;

/**
 * 目标页面实体
 */
public class TargetPageDataObject extends BaseDataObject {

    private List<TargetPageInfo> targetPageInfos;
    private PvDataObject pvDataObject;

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
