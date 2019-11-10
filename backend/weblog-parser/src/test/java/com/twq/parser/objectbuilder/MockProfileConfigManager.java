package com.twq.parser.objectbuilder;

import com.twq.metadata.api.ProfileConfigManager;
import com.twq.metadata.model.TargetPage;

import java.util.ArrayList;
import java.util.List;

public class MockProfileConfigManager implements ProfileConfigManager {
    @Override
    public List<TargetPage> loadAllTargetPagesConfig() {
        List<TargetPage> targetPages = new ArrayList<>();
        TargetPage targetPage = new TargetPage();
        targetPage.setEnable(true);
        targetPage.setMatchWithoutQueryString(true);
        targetPage.setMatchType("CONTAINS");
        targetPage.setMatchPattern("/checkoutLogin");
        targetPage.setProfileId(702);
        targetPage.setId("1");
        targetPage.setName("test target");
        targetPages.add(targetPage);
        return targetPages;
    }
}
