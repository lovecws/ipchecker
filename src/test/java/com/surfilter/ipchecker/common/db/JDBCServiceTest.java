package com.surfilter.ipchecker.common.db;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class JDBCServiceTest {

    JDBCService jdbcService = new JDBCService();

    @Test
    public void createTable() {
        String creatsql = "CREATE TABLE event_hubei_201806120926(ip varchar(32) not null,event_type varchar(32) not null,ip_operator varchar(32))";
        jdbcService.createTable(creatsql, null);
    }

    @Test
    public void tableExists() {
        jdbcService.tableExists("event_hubei_201806120926");
    }

    @Test
    public void dropTable() {
        jdbcService.dropTable("ipchecker_hubei_20180615");
    }

    @Test
    public void buildJangxiData() {
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        String modelPath = filePath + "/event/jiangxi/20180612160305/model.json";//匹配的事件列表
        jdbcService.buildJSONData(modelPath, "ipchecker_jiangxi_20180615", false);
    }

    @Test
    public void buildHubeiData() {
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        String modelPath = filePath + "/event/hubei/20180615142800/model.json";//匹配的事件列表
        jdbcService.buildJSONData(modelPath, "ipchecker_hubei_20180615", false);
    }

    @Test
    public void queryEventType() {
        List<Map<String, String>> mapList = jdbcService.query("ipchecker_hubei_20180612", "event_type");
        for (Map<String, String> result : mapList) {
            System.out.println(JSON.toJSONString(result));
        }
    }
}
