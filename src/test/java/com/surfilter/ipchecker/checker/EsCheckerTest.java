package com.surfilter.ipchecker.checker;

import org.junit.Test;

public class EsCheckerTest {

    @Test
    public void model() {
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        String eventPath = filePath + "/event/hubei/20180615142800/source.json";
        String modelPath = filePath + "/event/hubei/20180615142800/model.json";
        EsChecker esChecker = new EsChecker();
        esChecker.model(eventPath, modelPath, new String[]{"ip_operator","event_desc"});
    }
}
