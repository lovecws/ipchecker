package com.surfilter.ipchecker;

import com.surfilter.ipchecker.checker.EsChecker;
import com.surfilter.ipchecker.common.db.JDBCService;
import com.surfilter.ipchecker.data.EsDataService;
import com.surfilter.ipchecker.statistical.EventStatistical;
import com.surfilter.ipchecker.util.Pinyin4jUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class StartUp {

    public static final Logger log = Logger.getLogger(StartUp.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat longDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 开启source:es checker:es 任务
     *
     * @param sourceEsHost       source es 主机地址信息
     * @param sourceEsIndexName  source es 索引名称
     * @param sourceEsIndexType  source es 索引类型
     * @param sourceEsFieldName  source es 过滤字段名称
     * @param sourceEsFieldValue source es 过滤字段值
     * @param batchSize          批量处理数量
     * @param filePath           文件保存路径
     */
    public static void startEsSourceEsCheckerTask(String sourceEsHost, String sourceEsIndexName, String sourceEsIndexType, String sourceEsFieldName, String sourceEsFieldValue, int batchSize, String filePath) {
        if (sourceEsHost == null || sourceEsIndexName == null || sourceEsIndexType == null || sourceEsFieldName == null || sourceEsFieldValue == null) {
            throw new IllegalArgumentException("参数错误");
        }
        if (filePath == null) {
            filePath = System.getProperty("user.dir").replace("\\", "/");
        }
        String hanYuPinyinString = Pinyin4jUtil.toHanYuPinyinString(sourceEsFieldValue);
        filePath = filePath + "/event/" + hanYuPinyinString + "/" + longDateFormat.format(new Date());

        File eventDir = new File(filePath);
        if (!eventDir.exists()) {
            eventDir.mkdirs();
        }

        String sourcePath = filePath + "/source.json";//ip 文件列表
        String modelPath = filePath + "/model.json";//匹配的事件列表
        String statisticalPath = filePath + "/statistical.txt";//统计结果
        //从数据源录入的字段数据
        String[] sourceFiedls = new String[]{"ip_str",
                "location.city.zh-CN",
                "product.vendor",
                "product.vendorcn",
                "device.primary_type",
                "device.secondary_type",
                "service"};
        // 从es中获取数据 然后将抽取的ip数据写入到json文件中
        EsDataService esDataService = new EsDataService(sourceEsHost, sourceEsIndexName, sourceEsIndexType, sourceFiedls);
        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put(sourceEsFieldName, sourceEsFieldValue);
        esDataService.extractData(paramMap, batchSize, sourcePath);

        //使用eschecker来检测数据 并将结果写入到json文件中
        EsChecker esChecker = new EsChecker();
        //模型抽取
        String[] modelFields = new String[]{"ip_operator", "event_desc", "key"};
        esChecker.model(sourcePath, modelPath, modelFields);

        // 数据统计 将结果数据输出到文本文件中
        EventStatistical.statistical(modelPath, statisticalPath);

        //将数据结果写入到数据库中
        JDBCService jdbcService = new JDBCService();
        jdbcService.buildJSONData(modelPath, "ipchecker_" + hanYuPinyinString + "_" + dateFormat.format(new Date()), false);
    }

    public static void main(String[] args) {
        startEsSourceEsCheckerTask("http://172.31.134.229:9200", "wscan", "base", "location.region.zh-CN.untouched", "湖北", 1000, null);
    }
}
