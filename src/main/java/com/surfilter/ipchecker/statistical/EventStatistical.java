package com.surfilter.ipchecker.statistical;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.util.MapFieldUtil;

/**
 * 事件分析
 *
 * @author ganliang
 */
public class EventStatistical {

    public static final Logger log = Logger.getLogger(EventStatistical.class);

    public static void statistical(String sourcePath, String outputPath) {
        log.info("开始事件统计分析.................................................");
        if (sourcePath == null || outputPath == null) {
            throw new IllegalArgumentException();
        }

        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(new File(outputPath)));
            bufferedWriter.write("网络事件统计");
            bufferedWriter.newLine();
            bufferedWriter.newLine();

            // 统计事件类型
            Map<String, Integer> eventTypeMap = fieldCounter("eventDesc", sourcePath, null);
            writeEvent("事件类型统计结果", bufferedWriter, eventTypeMap);

            // 统计运营商
            Map<String, Integer> ipOperatorMap = fieldCounter("latestRecord.ip_operator", sourcePath, null);
            writeEvent("运营商统计结果", bufferedWriter, ipOperatorMap);

            // 统计事件类型下的运营商
            Map<String, Integer> eventTypeAndIpOperatorMap = fieldCounter("eventDesc,latestRecord.ip_operator", sourcePath,
                    null);
            writeEvent("事件类型下的运营商统计结果", bufferedWriter, eventTypeAndIpOperatorMap);

            // 统计运营商下的事件类型
            Map<String, Integer> ipOperatorAndEventTypeMap = fieldCounter("latestRecord.ip_operator,eventDesc", sourcePath,
                    null);
            writeEvent("运营商下的事件类型统计结果", bufferedWriter, ipOperatorAndEventTypeMap);
        } catch (Exception e) {
            log.error(e);
        } finally {
            try {
                if (bufferedWriter != null) bufferedWriter.close();
            } catch (Exception e) {
            }
            log.info("结束事件统计分析.................................................");
        }
    }

    /**
     * 运营商统计
     *
     * @param fieldName  统计的字段名称
     * @param sourcePath 源文件路径
     * @param outputPath 目标文件路径
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Map<String, Integer> fieldCounter(String fieldName, String sourcePath, String outputPath) {
        if (fieldName == null || "".equals(fieldName)) {
            throw new IllegalArgumentException();
        }
        if (sourcePath == null || "".equals(sourcePath)) {
            throw new IllegalArgumentException();
        }
        //使用treeMap排序
        Map<String, Integer> STATISTICAL_COUNTER = new TreeMap<String, Integer>(new Comparator<String>() {
            public int compare(String obj1, String obj2) {
                return obj2.compareTo(obj1);
            }
        });
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(sourcePath)));
            while ((readLine = bufferedReader.readLine()) != null) {
                Map recordMap = JSON.parseObject(readLine, Map.class);
                String url = recordMap.keySet().toArray()[0].toString();
                List<Map> events = (List<Map>) recordMap.get(url);
                for (Map event : events) {
                    //获取到统计字段
                    String statisticalField = MapFieldUtil.getStatisticalField(event, fieldName);
                    if (statisticalField == null) {
                        continue;
                    }
                    Integer COUNTER = STATISTICAL_COUNTER.get(statisticalField);
                    if (COUNTER == null || COUNTER == 0) {
                        COUNTER = 1;
                    } else {
                        COUNTER++;
                    }
                    STATISTICAL_COUNTER.put(statisticalField, COUNTER);
                }
            }
            if (outputPath != null && !"".equals(outputPath)) {
                bufferedWriter = new BufferedWriter(new FileWriter(new File(outputPath)));
                for (Entry entry : STATISTICAL_COUNTER.entrySet()) {
                    bufferedWriter.write(JSON.toJSONString(entry));
                }
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            try {
                if (bufferedReader != null)
                    bufferedReader.close();
                if (bufferedWriter != null)
                    bufferedWriter.close();
            } catch (IOException e) {
            }
        }
        return STATISTICAL_COUNTER;
    }

    /**
     * 将事件统计信息写入到文件中
     *
     * @param title
     * @param bufferedWriter
     * @param resultMap
     */
    public static void writeEvent(String title, BufferedWriter bufferedWriter, Map<String, Integer> resultMap) {
        try {
            bufferedWriter.write(title);
            bufferedWriter.newLine();
            for (Entry<String, Integer> entry : resultMap.entrySet()) {
                bufferedWriter.write(entry.getKey() + " : " + entry.getValue());
                log.info(title + " " + entry);
                bufferedWriter.newLine();
            }
            bufferedWriter.newLine();
            bufferedWriter.newLine();
        } catch (Exception e) {
            log.error(e);
        }

    }
}