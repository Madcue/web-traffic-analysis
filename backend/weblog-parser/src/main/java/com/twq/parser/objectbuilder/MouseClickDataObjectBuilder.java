package com.twq.parser.objectbuilder;

import com.twq.parser.dataobject.BaseDataObject;
import com.twq.parser.dataobject.McDataObject;
import com.twq.parser.utils.ColumnReader;
import com.twq.parser.utils.ParserUtils;
import com.twq.parser.utils.UrlInfo;
import com.twq.prepaser.PreParsedLog;

import java.util.ArrayList;
import java.util.List;

import static com.twq.parser.utils.UrlParseUtils.getInfoFromUrl;

public class MouseClickDataObjectBuilder extends AbstractDataObjectBuilder {

    private static int clickXBoundary = 10000;
    private static int clickYBoundary = 100000;

    @Override
    public String getCommand() {
        return "mc";
    }

    @Override
    public List<BaseDataObject> doBuildDataObjects(PreParsedLog preParsedLog) {
        List<BaseDataObject> dataObjects = new ArrayList<>();
        //解析并填充公用字段
        McDataObject mcDataObject = new McDataObject();
        ColumnReader reader = new ColumnReader(preParsedLog.getQueryString());
        fillCommonBaseDataObjectValue(mcDataObject, preParsedLog, reader);

        //解析mc并填充特有的字段
        //点击页面的信息
        UrlInfo urlInfo = getInfoFromUrl(reader.getStringValue("gsmcurl"));
        mcDataObject.setUrl(urlInfo.getFullUrl());
        mcDataObject.setOriginalUrl(reader.getStringValue("gsorurl"));
        mcDataObject.setPageTitle(reader.getStringValue("gstl"));
        mcDataObject.setPageHostName(urlInfo.getDomain());
        mcDataObject.setPageRegion(getIntValue("re", reader));
        mcDataObject.setPageVersion(reader.getStringValue("pageVersion"));
        mcDataObject.setSnapshotId(Integer.parseInt(reader.getStringValue("gssn")));
        //点击的x和y值
        int accurateClickX = Integer.parseInt(reader.getStringValue("gsmcoffsetx"));
        int accurateClickY = Integer.parseInt(reader.getStringValue("gsmcoffsety"));
        mcDataObject.setClickX(accurateClickX);
        mcDataObject.setClickY(accurateClickY);
        //点击的链接信息
        mcDataObject.setLinkX(getValidClickXYPoint(getIntValue("lx", reader), clickXBoundary, -1 * clickXBoundary));
        mcDataObject.setLinkY(getValidClickXYPoint(getIntValue("ly", reader), clickYBoundary, 0));
        mcDataObject.setLinkHeight(getIntValue("lh", reader));
        mcDataObject.setLinkWidth(getIntValue("lw", reader));
        mcDataObject.setLinkText(reader.getStringValue("lt"));
        mcDataObject.setLinkUrl(reader.getStringValue("lk"));
        mcDataObject.setLinkHostName(getInfoFromUrl(mcDataObject.getLinkUrl()).getDomain());
        if (!ParserUtils.isNullOrEmptyOrDash(mcDataObject.getLinkUrl()) ||
                !ParserUtils.isNullOrEmptyOrDash(mcDataObject.getLinkText())) {
            mcDataObject.setLinkClicked(true);
        }
        //分辨率
        mcDataObject.setClickScreenResolution(reader.getStringValue("gsscr"));

        dataObjects.add(mcDataObject);
        return dataObjects;
    }

    private Integer getIntValue(String key, ColumnReader columnReader) {
        String value = columnReader.getStringValue(key);
        if (!ParserUtils.isNullOrEmptyOrDash(value)) {
            return Integer.parseInt(value);
        } else {
            return 0;
        }
    }


    private int getValidClickXYPoint(int point, int maxBoundaryValue,
                                     int minBoundaryValue) {
        if (point < minBoundaryValue) {
            return minBoundaryValue;
        } else if (point > maxBoundaryValue) {
            return maxBoundaryValue;
        } else {
            return point;
        }
    }
}
