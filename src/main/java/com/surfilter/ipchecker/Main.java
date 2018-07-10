package com.surfilter.ipchecker;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;

public class Main {

    public static final Logger log = Logger.getLogger(Main.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat longDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 开始湖北任务
     */
    public static void startHubeiTask() {
        TaskService taskService = new TaskService();
        taskService.startSimpleTask("http://172.31.134.229:9200",
                "wscan",
                "base",
                "location.region.zh-CN.untouched",
                "湖北",
                1000,
                null);
    }

    public static void startJiangxiTask() {
        TaskService exploitMain = new TaskService(true, "hdfs", true, true, true, true);
        exploitMain.startTask("http://172.31.134.229:9200",
                "wscan",
                "base",
                "location.region.zh-CN.untouched",
                "江西",
                1000,
                null);
    }

    public static void startAnhuiTask() {
        TaskService exploitMain = new TaskService(false, null, true, false, true, true);
        exploitMain.startTask("http://172.31.134.229:9200",
                "wscan",
                "base",
                "location.region.zh-CN.untouched",
                "安徽",
                1000,
                null);
    }

    public static void main(String[] args) {
        startAnhuiTask();
    }
}
