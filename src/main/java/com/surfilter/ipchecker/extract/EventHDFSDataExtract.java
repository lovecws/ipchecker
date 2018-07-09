package com.surfilter.ipchecker.extract;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class EventHDFSDataExtract {

    public static final Logger log = Logger.getLogger(EventHDFSDataExtract.class);

    /**
     * 往数据模型里面添加 运营商和僵木控制字段
     *
     * @param modelPath   源模型文件
     * @param checkerPath 匹配文件
     * @param ipModelPath 输出文件
     */
    public void extract(String modelPath, String checkerPath, String ipModelPath) {
        if (modelPath == null || checkerPath == null || ipModelPath == null) {
            throw new IllegalArgumentException();
        }
        BufferedReader bufferedReader = null;
        BufferedReader checkerBufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(modelPath)));
            checkerBufferedReader = new BufferedReader(new FileReader(new File(checkerPath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File(ipModelPath)));
            //读取僵木事件信息表 将数据抽取到map中
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
            log.info(recordMap);
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
            log.error(e1);
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
