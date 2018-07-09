package com.surfilter.ipchecker.extract;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * 检测ip
 */
public abstract class BaseDataExtract {

    public static final Logger log = Logger.getLogger(BaseDataExtract.class);

    /**
     * @param sourcePath  ip来源文件
     * @param modelPath   模型保存文件
     * @param modelFields 模型字段 ip_operator、count、event_desc
     *                    {"e_detail_type":"其他木马",
     *                    "e_month":201712,
     *                    "e_extend_type":"木马-其他-Trojan.Delfinject.R.217B_特征1",
     *                    "submit_time":"2017-12-01 16:58:01",
     *                    "src_ip_area":"湖北",
     *                    "src_ip_operator":"电信",
     *                    "e_date":20171201,
     *                    "dst_ip_point":"115.236.23.119",
     *                    "src_port":34977,
     *                    "dst_ip_area":"浙江",
     *                    "src_ip_point":"221.234.12.150",
     *                    "attack_num":1,
     *                    "e_sub_type":"木马",
     *                    "dst_ip_value":1944852343,
     *                    "dst_port":80,
     *                    "unit_code":"",
     *                    "src_ip_value":3723103382,
     *                    "e_year":2017,
     *                    "using_unit_name":"应山宾馆",
     *                    "use_unit":"",
     *                    "key":"767e513f443887b920171201"}
     */
    public abstract void extract(String sourcePath, String modelPath, String[] modelFields);

    /**
     * 将ip匹配到的事件写入到json文件中
     *
     * @param sourceMap      匹配的事件集合
     * @param bufferedWriter
     */
    public void eventRecord(Map<String, Object> sourceMap, BufferedWriter bufferedWriter) {
        try {
            String jsonString = JSON.toJSONString(sourceMap);
            log.info(jsonString);
            bufferedWriter.write(jsonString);
            bufferedWriter.newLine();
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
