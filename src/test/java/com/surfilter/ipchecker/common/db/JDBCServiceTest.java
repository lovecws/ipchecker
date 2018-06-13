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
        jdbcService.createTable(creatsql);
    }

    @Test
    public void tableExists() {
        jdbcService.tableExists("event_hubei_201806120926");
    }

    @Test
    public void dropTable() {
        jdbcService.dropTable("IPCHECKER_HUBEI_20180612");
    }

    @Test
    public void buildJangxiData() {
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        jdbcService.buildData(filePath + "/event/jiangxi/20180612160305/part-00000", "ipchecker_jiangxi_20180612", new String[]{"ip", "ipOperator", "eventType"}, ",",true);
    }

    @Test
    public void buildHubeiData() {
        String filePath = System.getProperty("user.dir").replace("\\", "/");
        jdbcService.buildData(filePath + "/event/hubei/20180612181436/model.csv", "ipchecker_hubei_20180612", new String[]{"ip", "ipOperator", "eventType"}, " ",false);
    }

    @Test
    public void queryEventType() {
        List<Map<String, String>> mapList = jdbcService.query("ipchecker_hubei_20180612", "event_type");
        for (Map<String, String> result : mapList) {
            System.out.println(JSON.toJSONString(result));
        }
    }
}
