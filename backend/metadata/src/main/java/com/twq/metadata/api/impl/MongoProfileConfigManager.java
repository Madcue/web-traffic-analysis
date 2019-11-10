package com.twq.metadata.api.impl;

import com.mongodb.Block;
import com.mongodb.client.MongoDatabase;
import com.twq.metadata.MongoDB;
import com.twq.metadata.api.ProfileConfigManager;
import com.twq.metadata.model.TargetPage;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoProfileConfigManager implements ProfileConfigManager {
    /**
     * 从mongo中加载所有的目标页面配置
     * @return
     */
    public List<TargetPage> loadAllTargetPagesConfig() {
        final List<TargetPage> targetPages = new ArrayList<TargetPage>();

        MongoDatabase database = MongoDB.getMongoDatabase("web-analysis");
        database.getCollection("TargetPage").find().forEach(new Block<Document>() {
            @Override
            public void apply(Document document) {
                TargetPage targetPage = new TargetPage();
                targetPage.setId(document.getString("Id"));
                targetPage.setProfileId(document.getInteger("ProfileId"));
                targetPage.setName(document.getString("Name"));
                targetPage.setDescription(document.getString("Description"));
                targetPage.setMatchPattern(document.getString("MatchPattern"));
                targetPage.setMatchType(document.getString("MatchType"));
                targetPage.setMatchWithoutQueryString(document.getBoolean("MatchWithoutQueryString"));
                targetPage.setEnable(!document.getBoolean("IsDisabled"));

                targetPages.add(targetPage);
            }
        });
        return targetPages;
    }
}
