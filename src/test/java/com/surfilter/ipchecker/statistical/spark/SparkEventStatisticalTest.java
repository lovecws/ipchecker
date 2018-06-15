package com.surfilter.ipchecker.statistical.spark;

import org.junit.Test;

public class SparkEventStatisticalTest {

    @Test
    public void statistical(){
        SparkEventStatistical sparkEventStatistical=new SparkEventStatistical();

        String projectDir = System.getProperty("user.dir").replace("\\", "/");
        sparkEventStatistical.statistical(projectDir+"/event/hubei/20180612181436/event.json","eventType,latestRecord.ip_operator",null);
    }

    @Test
    public void jiangxiStatistical(){
        SparkEventStatistical sparkEventStatistical=new SparkEventStatistical();

        String projectDir = System.getProperty("user.dir").replace("\\", "/");
        String modelPath=projectDir+"/event/jiangxi/20180612160305/part-00000";

        //统计事件类型
        sparkEventStatistical.statisticalFromModel(modelPath,"2",null);

        //统计运营商
        sparkEventStatistical.statisticalFromModel(modelPath,"1",null);

        //统计事件类型下的运营商
        sparkEventStatistical.statisticalFromModel(modelPath,"2,1",null);

        //统计运营商下的事件类型
        sparkEventStatistical.statisticalFromModel(modelPath,"2,1",null);
    }
}
