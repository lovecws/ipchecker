package com.surfilter.ipchecker.checker;

import org.junit.Test;

public class EsCheckerTest {

    @Test
    public void check(){
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        String ipPath=filePath+"/event/hubei/20180612131904/ip.txt";
        String eventPath=filePath+"/event/hubei/20180612131904/event.json";
        EsChecker esChecker=new EsChecker();
        esChecker.check(ipPath,eventPath);
    }

    @Test
    public void model(){
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        String eventPath=filePath+"/event/hubei/20180612181436/event.json";
        String modelPath=filePath+"/event/hubei/20180612181436/model.csv";
        EsChecker esChecker=new EsChecker();
        esChecker.model(eventPath,modelPath);
    }
}
