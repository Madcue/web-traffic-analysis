package com.twq.parser.dataobject;

public class InvalidLogObject implements ParsedDataObject {
    private String event;

    public InvalidLogObject(String event) {
        this.event = event;
    }

    public String getEvent() {
        return event;
    }
}
