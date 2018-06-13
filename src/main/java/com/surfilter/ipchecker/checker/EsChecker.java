package com.surfilter.ipchecker.checker;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.data.EsDataService;
import com.surfilter.ipchecker.entity.EventRecordEntity;
import com.surfilter.ipchecker.util.EventUtil;
import com.surfilter.ipchecker.util.MapFieldUtil;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用es来检测数据
 */
public class EsChecker extends BaseChecker {

    public static final Logger log = Logger.getLogger(EsChecker.class);

    @Override
    public void check(String ipPath, String eventPath) {
        log.info("开始ip检测.................................................");
        if (ipPath == null || eventPath == null) {
            throw new IllegalArgumentException();
        }
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(ipPath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File(eventPath)));

            while ((readLine = bufferedReader.readLine()) != null) {
                if (readLine == null || "".equals(readLine)) {
                    continue;
                }
                //解析文本 每行文本都是一个ip  使用ip来匹配es索引
                List<EventRecordEntity> eventRecords = EventUtil.event(readLine, null);
                if (eventRecords == null || eventRecords.size() == 0) {
                    continue;
                }
                Map<String, List<EventRecordEntity>> eventRecordMap = new HashMap<String, List<EventRecordEntity>>();
                eventRecordMap.put(readLine, eventRecords);
                //将事件记录写入到json文件中
                eventRecord(eventRecordMap, bufferedWriter);
            }
        } catch (Exception e1) {
            log.error(e1);
        } finally {
            try {
                if (bufferedReader != null) bufferedReader.close();
                if (bufferedWriter != null) bufferedWriter.close();
            } catch (IOException e) {
                log.error(e);
            }
            log.info("结束ip检测.................................................");
        }
    }

    @Override
    public void model(String eventPath, String modelPath) {
        log.info("开始模型抽取.................................................");
        if (eventPath == null || modelPath == null) {
            throw new IllegalArgumentException();
        }
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(eventPath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File(modelPath)));

            while ((readLine = bufferedReader.readLine()) != null) {
                if (readLine == null || "".equals(readLine)) {
                    continue;
                }
                Map recordMap = JSON.parseObject(readLine, Map.class);
                String url = recordMap.keySet().toArray()[0].toString();
                List<Map> events = (List<Map>) recordMap.get(url);
                for (Map event : events) {
                    String eventDesc = event.get("eventDesc").toString();
                    String temFieldName = "latestRecord.ip_operator";
                    String eventType = event.get("eventType").toString();
                    if ("worm".equalsIgnoreCase(eventType)) {
                        temFieldName = temFieldName.replace("ip_operator", "ip_operator");
                    } else if ("bot_or_trojan".equals(eventType)) {
                        temFieldName = temFieldName.replace("ip_operator", "src_ip_operator");
                    } else if ("bot_or_trojan_c".equals(eventType)) {
                        temFieldName = temFieldName.replace("ip_operator", "dst_ip_operator");
                    }
                    String ipOperator = MapFieldUtil.getFieldValue(event, temFieldName);
                    String baseLine = url + " " + ipOperator + " " + eventDesc;
                    bufferedWriter.write(baseLine);
                    bufferedWriter.newLine();
                    log.info(baseLine);
                }
            }
        } catch (Exception e1) {
            log.error(e1);
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
