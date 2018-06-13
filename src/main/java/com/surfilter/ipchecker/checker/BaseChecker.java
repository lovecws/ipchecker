package com.surfilter.ipchecker.checker;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.entity.EventRecordEntity;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 检测ip
 */
public abstract class BaseChecker {

    public static final Logger log = Logger.getLogger(BaseChecker.class);

    public abstract void check(String ipPath,String eventPath);
    public abstract void model(String eventPath, String baseEventPath);

    /**
     * 将ip匹配到的事件写入到json文件中
     * @param eventRecordMap 匹配的事件集合
     * @param bufferedWriter
     */
    public void eventRecord(Map<String, List<EventRecordEntity>> eventRecordMap, BufferedWriter bufferedWriter){
        try {
            for(Map.Entry<String,List<EventRecordEntity>> entry:eventRecordMap.entrySet()){
                String jsonString = JSON.toJSONString(entry);
                log.info(jsonString);
                bufferedWriter.write(jsonString);
                bufferedWriter.newLine();
            }
        } catch (Exception e1) {
            log.error(e1);
        } finally {
            try {
                bufferedWriter.flush();
            } catch (IOException e) {
                log.error(e);
            }
        }
    }
}
