package com.twq.parser.configuration.loader;

import com.twq.metadata.model.TargetPage;

import java.util.List;
import java.util.Map;

public interface ProfileConfigLoader {
    public Map<Integer,List<TargetPage>> getTargetPages();
}
