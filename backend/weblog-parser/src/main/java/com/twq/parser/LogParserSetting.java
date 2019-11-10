package com.twq.parser;

import java.util.Set;

public class LogParserSetting {
    //LogParser支持的日志类型
    private Set<String> cmds;

    public LogParserSetting(Set<String> cmds) {
        this.cmds = cmds;
    }

    public Set<String> getCmds() {
        return cmds;
    }
}
