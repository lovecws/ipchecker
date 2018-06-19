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

    @Test
    public void mix() {
        String filePath = System.getProperty("user.dir").replace("\\", "/") + "/event/jiangxi/20180612160305";
        String sourcePath = filePath + "/source.json";
        String checkerPath = filePath + "/part-00000";
        String modelPath = filePath + "/model.json";
        BufferedReader bufferedReader = null;
        BufferedReader checkerBufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(sourcePath)));
            checkerBufferedReader = new BufferedReader(new FileReader(new File(checkerPath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File(modelPath)));

            Map<String, Map<String, String>> recordMap = new HashMap<String, Map<String, String>>();
            while ((readLine = checkerBufferedReader.readLine()) != null) {
                if (readLine == null || "".equals(readLine)) {
                    continue;
                }
                String[] strings = readLine.split(",");
                String ip_str = strings[0];
                String ip_operator = strings[1];
                String event_desc = strings[2];
                if ("0".equals(event_desc)) {
                    continue;
                }
                if ("1".equals(event_desc)) {
                    event_desc = "僵木控制";
                }
                Map<String, String> resultMap = new HashMap<>();
                resultMap.put("ip_operator", ip_operator);
                resultMap.put("event_desc", event_desc);
                Map<String, String> stringStringMap = recordMap.get(ip_str);
                if (stringStringMap == null) {
                    recordMap.put(ip_str, resultMap);
                }
            }
            System.out.println(recordMap);
            //读取源文件
            while ((readLine = bufferedReader.readLine()) != null) {
                Map<String, Object> sourceMap = JSON.parseObject(readLine, Map.class);
                Map<String, String> stringStringMap = recordMap.get(sourceMap.get("ip_str").toString());
                if (stringStringMap != null && stringStringMap.size() > 0) {
                    sourceMap.putAll(stringStringMap);
                } else {
                    sourceMap.put("ip_operator", "");
                    sourceMap.put("event_desc", "");
                }
                String jsonString = JSON.toJSONString(sourceMap);
                bufferedWriter.write(jsonString);
                bufferedWriter.newLine();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) bufferedReader.close();
                if (checkerBufferedReader != null) checkerBufferedReader.close();
                if (bufferedWriter != null) bufferedWriter.close();
            } catch (IOException e) {
            }
        }
    }
}
