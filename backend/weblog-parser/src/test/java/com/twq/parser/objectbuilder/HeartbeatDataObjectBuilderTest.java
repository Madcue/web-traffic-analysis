package com.twq.parser.objectbuilder;

import com.twq.parser.dataobject.BaseDataObject;
import com.twq.parser.dataobject.EventDataObject;
import com.twq.parser.dataobject.HeartbeatDataObject;
import com.twq.prepaser.PreParsedLog;
import com.twq.prepaser.WebLogPreParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class HeartbeatDataObjectBuilderTest {

    private HeartbeatDataObjectBuilder heartbeatDataObjectBuilder = new HeartbeatDataObjectBuilder();

    @Test
    public void doBuildDataObject() {
        String weblog = "2018-06-15 19:43:28 10.200.200.98 GET /gs.gif " +
                "gsver=3.7.0.14&gscmd=hb&gssrvid=GWD-002249&gsuid=288723943z1r7257&gssid=29139924x52ybq76" +
                "&pvid=2914025643u3cy61&gsltime=1529169209542&gstmzone=8&rd=fd450&plt=1.711&pld=0.317&psd=153.123" +
                "&gsst=0&gswh=759 80 - 58.210.35.226 " +
                "Mozilla/5.0+(Windows+NT+10.0;+Win64;+x64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/67.0.3396.87+Safari/537.36";
        PreParsedLog preParsedLog = WebLogPreParser.parse(weblog);
        List<BaseDataObject> dataObjects = heartbeatDataObjectBuilder.doBuildDataObjects(preParsedLog);
        Assert.assertEquals(1, dataObjects.size());
        HeartbeatDataObject heartbeatDataObject = (HeartbeatDataObject)dataObjects.get(0);
        Assert.assertEquals(2, heartbeatDataObject.getLoadingDuration());
        Assert.assertEquals(153, heartbeatDataObject.getClientPageDuration());
    }
}