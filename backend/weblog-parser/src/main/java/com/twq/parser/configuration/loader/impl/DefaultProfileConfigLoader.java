package com.twq.parser.configuration.loader.impl;

import com.twq.metadata.api.ProfileConfigManager;
import com.twq.metadata.model.TargetPage;
import com.twq.parser.configuration.loader.ProfileConfigLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DefaultProfileConfigLoader implements ProfileConfigLoader{
    private Map<Integer, List<TargetPage>> profileTargetPages = new HashMap<>();

    public DefaultProfileConfigLoader(ProfileConfigManager profileConfigManager){
     List<TargetPage> allTargetPages =  profileConfigManager.loadAllTargetPagesConfig();
     allTargetPages.forEach(new Consumer<TargetPage>() {
         @Override
         public void accept(TargetPage targetPage) {
             List<TargetPage> existsTgs = profileTargetPages.get(targetPage.getProfileId());
             if(existsTgs == null){
                 List<TargetPage> newTgs = new ArrayList<>();
                 newTgs.add(targetPage);
                 profileTargetPages.put(targetPage.getProfileId(),newTgs);
             }else {
                 existsTgs.add(targetPage);
             }
         }
     });
    }

    @Override
    public Map<Integer, List<TargetPage>> getTargetPages() {
        return profileTargetPages;
    }
}
