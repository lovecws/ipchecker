package com.surfilter.ipchecker.extract;

import com.surfilter.ipchecker.common.db.JDBCService;
import com.surfilter.ipchecker.extract.IPUnitDataExtract;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class IPUnitDataExtractTest {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Test
    public void model() {
        String filePath = System.getProperty("user.dir").replace("\\", "/") + "/event/jiangxi/20180620110434";
        String exploitModelPath = filePath + "/exploit_model.json";//匹配的漏洞
        String ipUnitModelPath = filePath + "/ipunit_model.json";//根据ipstr匹配ip单位

        IPUnitDataExtract ipUnitDataExtract = new IPUnitDataExtract("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@localhost:11521:orcl", "smcs_jx", "smcs");
        ipUnitDataExtract.extract(exploitModelPath, ipUnitModelPath);
    }

    @Test
    public void buildJSONData() {
        String filePath = System.getProperty("user.dir").replace("\\", "/") + "/event/jiangxi/20180620110434";
        String ipUnitModelPath = filePath + "/ipunit_model.json";//根据ipstr匹配ip单位
        String exploitModelPath = filePath + "/exploit_model.json";//匹配的漏洞
        //将统计出来的结果保存到oracle数据库中
        JDBCService jdbcService = new JDBCService();
        jdbcService.buildJSONData(ipUnitModelPath, "ipchecker_jiangxi_" + dateFormat.format(new Date()), false);
    }
}
