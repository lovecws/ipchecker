package com.surfilter.ipchecker.extract;

import org.junit.Test;

public class EventESDataExtractTest {

    @Test
    public void model() {
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        String eventPath = filePath + "/event/hubei/20180615142800/source.json";
        String modelPath = filePath + "/event/hubei/20180615142800/model.json";
        EventESDataExtract eventESDataExtract = new EventESDataExtract();
        eventESDataExtract.extract(eventPath, modelPath, new String[]{"ip_operator","event_desc"});
    }
}
