package com.surfilter.ipchecker.data;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.util.HttpClientUtil;
import com.surfilter.ipchecker.util.MapFieldUtil;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

/**
 * 从es中抽取数据
 *
 * @author ganliang
 */
public class EsDataService extends BaseDataService {

    public String ELASTICSEARCH_BASE_URL = null;
    public String ELASTICSEARCH_WSCAN_URL = null;
    public static final Logger log = Logger.getLogger(EsDataService.class);


    /**
     * 使用滚动api来查询
     *
     * @param paramMap 查询字段名称、值
     * @param size     每次批量处理数量
     * @return
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void extractData(Map<String, Object> paramMap, int size, String filePath) {
        log.info("开始从es中抽取数据.................................................");
        // 使用scroll api调用 获取scroll
        String baseURL = ELASTICSEARCH_WSCAN_URL + "/_search?scroll=5m&search_type=scan&size=" + size + "&q=";

        StringBuilder paramBuilder = new StringBuilder();
        Iterator<Map.Entry<String, Object>> iterator = paramMap.entrySet().iterator();
        //+ 或者
        // & 并且
        //+- 包含不包含
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            paramBuilder.append(entry.getKey() + ":" + entry.getValue());
            if (iterator.hasNext()) {
                paramBuilder.append("+");
            }
        }
        baseURL = baseURL + paramBuilder.toString();
        String result = HttpClientUtil.get(baseURL);
        Map scrollResultMap = JSON.parseObject(result, Map.class);
        log.info(scrollResultMap);
        String _scroll_id = scrollResultMap.get("_scroll_id").toString();
        Map scrollHitMap = (Map) scrollResultMap.get("hits");
        log.info("total record:" + scrollHitMap.get("total"));

        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(filePath));
            while (true) {
                result = HttpClientUtil
                        .get(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll=5m&scroll_id=" + _scroll_id);
                Map resultMap = JSON.parseObject(result, Map.class);
                _scroll_id = resultMap.get("_scroll_id").toString();
                List<String> records = parseResult(resultMap);
                if (records == null) {
                    break;
                }
                ipRecord(records, bufferedWriter);
            }
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (Exception e) {
            }
            // 关闭 scroll
            HttpClientUtil.delete(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll_id=" + _scroll_id);
            log.info("结束从es中抽取数据.................................................");
        }
    }

    /**
     * 解析es返回结果
     *
     * @param resultMap
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<String> parseResult(Map resultMap) {
        Map hitMap = (Map) resultMap.get("hits");

        List<Map> records = (List<Map>) hitMap.get("hits");
        if (records.size() == 0) {
            return null;
        }
        List<String> ips = new ArrayList<String>();
        for (Map recordMap : records) {
            Map _sourceMap = (Map) recordMap.get("_source");
            Map<String, Object> result = new HashMap<String, Object>();
            for (String recordField : recordFields) {
                String fieldValue = MapFieldUtil.getFieldValue(_sourceMap, recordField);
                result.put(recordField, fieldValue);
            }
            ips.add(JSON.toJSONString(result));
        }
        return ips;
    }

    /**
     * 使用rest接口获取es数据
     */
    @SuppressWarnings({"rawtypes"})
    public List<String> getIps(String fieldName, String fieldValue, int size, String filePath) {
        String result = HttpClientUtil.get(ELASTICSEARCH_WSCAN_URL + "/_search?scroll=5m&search_type=scan&size=" + size
                + "&q=" + fieldName + ":" + fieldValue);
        Map scrollResultMap = JSON.parseObject(result, Map.class);
        log.info(scrollResultMap);
        String _scroll_id = scrollResultMap.get("_scroll_id").toString();
        Map scrollHitMap = (Map) scrollResultMap.get("hits");
        log.info("total record:" + scrollHitMap.get("total"));

        List<String> totalIps = new ArrayList<String>();
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(filePath));
            while (true) {
                result = HttpClientUtil
                        .get(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll=5m&scroll_id=" + _scroll_id);
                Map resultMap = JSON.parseObject(result, Map.class);
                _scroll_id = resultMap.get("_scroll_id").toString();
                List<String> ips = parseResult(resultMap);
                if (ips == null) {
                    break;
                }
                for (String record : ips) {
                    bufferedWriter.write(record);
                    bufferedWriter.newLine();
                }
                bufferedWriter.flush();
                totalIps.addAll(ips);
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (Exception e) {
            }
            // 关闭 scroll
            HttpClientUtil.delete(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll_id=" + _scroll_id);
        }
        return totalIps;
    }

    private String host;
    private String indexName;
    private String typeName;
    private String[] recordFields;

    public EsDataService(String host, String indexName, String typeName, String[] recordFields) {
        super();
        this.host = host;
        this.indexName = indexName;
        this.typeName = typeName;
        this.recordFields = recordFields;
        ELASTICSEARCH_BASE_URL = host;
        ELASTICSEARCH_WSCAN_URL = host + "/" + indexName + "/" + typeName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String[] getRecordFields() {
        return recordFields;
    }

    public void setRecordFields(String[] recordFields) {
        this.recordFields = recordFields;
    }
}
