package com.surfilter.ipchecker.extract;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.entity.EventRecordEntity;
import com.surfilter.ipchecker.event.EsEventService;
import com.surfilter.ipchecker.util.MapFieldUtil;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 使用es来检测数据
 */
public class EventESDataExtract extends BaseDataExtract {

    public static final Logger log = Logger.getLogger(EventESDataExtract.class);

    @Override
    public void extract(String sourcePath, String modelPath, String[] modelFields) {
        log.info("开始模型抽取.................................................");
        if (sourcePath == null || modelPath == null || modelFields == null) {
            throw new IllegalArgumentException();
        }
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(sourcePath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File(modelPath)));

            while ((readLine = bufferedReader.readLine()) != null) {
                if (readLine == null || "".equals(readLine)) {
                    continue;
                }
                Map sourceMap = JSON.parseObject(readLine, Map.class);
                //记录添加key关键字 区别重复数据
                sourceMap.put("key", UUID.randomUUID().toString().replace("-", ""));
                String ip_str = sourceMap.get("ip_str").toString();
                //解析文本 每行文本都是一个ip  使用ip来匹配es索引
                List<EventRecordEntity> eventRecords = EsEventService.esEvent(ip_str, null);
                if (eventRecords != null && eventRecords.size() > 0) {
                    for (EventRecordEntity eventRecordEntity : eventRecords) {
                        Map<String, Object> recordMap = new HashMap<String, Object>();
                        recordMap.putAll(sourceMap);
                        Map<String, Object> latestRecord = eventRecordEntity.getLatestRecord();
                        for (String modelField : modelFields) {
                            if (modelField == null) {
                                continue;
                            }
                            switch (modelField) {
                                case "ip_operator":
                                    String eventType = eventRecordEntity.getEventType();
                                    String temFieldName = modelField;
                                    if ("worm".equalsIgnoreCase(eventType)) {
                                        temFieldName = temFieldName.replace("ip_operator", "ip_operator");
                                    } else if ("bot_or_trojan".equals(eventType)) {
                                        temFieldName = temFieldName.replace("ip_operator", "src_ip_operator");
                                    } else if ("bot_or_trojan_c".equals(eventType)) {
                                        temFieldName = temFieldName.replace("ip_operator", "dst_ip_operator");
                                    }
                                    String ipOperator = MapFieldUtil.getFieldValue(latestRecord, temFieldName);
                                    recordMap.put(modelField, ipOperator);
                                    break;
                                case "event_desc":
                                    String eventDesc = eventRecordEntity.getEventDesc();
                                    recordMap.put(modelField, eventDesc);
                                    break;
                                case "count":
                                    long count = eventRecordEntity.getCount();
                                    recordMap.put(modelField, count);
                                    break;
                                default:
                                    String modelValue = MapFieldUtil.getFieldValue(latestRecord, modelField);
                                    recordMap.put(modelField, modelValue);
                                    break;
                            }
                        }
                        eventRecord(recordMap, bufferedWriter);
                    }
                } else {
                    sourceMap.put("event_desc", "");
                    sourceMap.put("ip_operator", "");
                    eventRecord(sourceMap, bufferedWriter);
                }
            }
        } catch (Exception e1) {
            log.error(e1);
            e1.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) bufferedReader.close();
                if (bufferedWriter != null) bufferedWriter.close();
            } catch (IOException e) {
                log.error(e);
            }
            log.info("结束模型抽取.................................................");
        }
    }
}
