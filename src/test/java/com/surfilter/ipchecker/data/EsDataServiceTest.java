package com.surfilter.ipchecker.data;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EsDataServiceTest {

    @Test
    public void getIps() {
        EsDataService esDataService = new EsDataService("http://172.31.134.229:9200", "wscan", "base", new String[]{"ip_str"});

        String filePath = System.getProperty("user.dir").replace("\\", "/");
        filePath = filePath + "/event/jiangxi";
        File eventDir = new File(filePath);
        if (!eventDir.exists()) {
            eventDir.mkdirs();
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateString = sdf.format(new Date());
        String sourcePath = filePath + "/ip" + dateString + ".txt";
        esDataService.getIps("location.region.zh-CN.untouched", "江西", 1000, sourcePath);
    }

    @Test
    public void extractData() {
        //从数据源录入的字段数据
        String[] sourceFiedls = new String[]{"ip_str",
                "location.city.zh-CN",
                "product.vendor",
                "product.vendorcn",
                "device.primary_type",
                "device.secondary_type",
                "service"};
        String sourcePath = System.getProperty("user.dir").replace("\\", "/") + "/event/jiangxi/20180612160305" + "/source.json";

        // 从es中获取数据 然后将抽取的ip数据写入到json文件中
        EsDataService esDataService = new EsDataService("http://172.31.134.229:9200", "wscan", "base", sourceFiedls);

        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("location.region.zh-CN.untouched", "江西");
        esDataService.extractData(paramMap, 1000, sourcePath);
    }
}
