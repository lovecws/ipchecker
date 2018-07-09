package com.surfilter.ipchecker.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.surfilter.ipchecker.Main;

/**
 * 从各种不同的来源获取到ip数据集合 将ip集合写入到文本文件中
 */
public abstract class BaseDataService {

    public static final Logger log = Logger.getLogger(Main.class);

    public abstract void extractData(Map<String, Object> paramMap, int size, String filePath);

    /**
     * 将ip数据写入到文本文件中
     *
     * @param ips            ip集合
     * @param bufferedWriter
     */
    public void ipRecord(List<String> ips, BufferedWriter bufferedWriter) {
        try {
            for (String record : ips) {
                log.info(record);
                bufferedWriter.write(record);
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
