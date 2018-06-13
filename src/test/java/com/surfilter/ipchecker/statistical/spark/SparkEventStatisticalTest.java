package com.surfilter.ipchecker.statistical.spark;

import org.junit.Test;

public class SparkEventStatisticalTest {

    @Test
    public void statistical(){
        SparkEventStatistical sparkEventStatistical=new SparkEventStatistical();

        String projectDir = System.getProperty("user.dir").replace("\\", "/");
        sparkEventStatistical.statistical(projectDir+"/event/hubei/20180612181436/event.json","eventType,latestRecord.ip_operator",null);
    }
}
