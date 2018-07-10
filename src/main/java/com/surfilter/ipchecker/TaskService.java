package com.surfilter.ipchecker;

import com.surfilter.ipchecker.common.db.JDBCService;
import com.surfilter.ipchecker.data.EsDataService;
import com.surfilter.ipchecker.extract.EventESDataExtract;
import com.surfilter.ipchecker.extract.EventHDFSDataExtract;
import com.surfilter.ipchecker.extract.ExploitDataExtract;
import com.surfilter.ipchecker.extract.IPUnitDataExtract;
import com.surfilter.ipchecker.statistical.EventStatistical;
import com.surfilter.ipchecker.statistical.spark.ExploitStatistical;
import com.surfilter.ipchecker.util.Pinyin4jUtil;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 甘亮
 * @Description: 任务
 * @date 2018/7/9 17:29
 */
public class TaskService {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat longDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * @param sourceEsHost       源es主机
     * @param sourceEsIndexName  源es索引
     * @param sourceEsIndexType  源es类型
     * @param sourceEsFieldName  源es字段名称
     * @param sourceEsFieldValue 源es字段值
     * @param batchSize          批量数量
     * @param filePath           文件保存路径
     */
    public void startTask(String sourceEsHost, String sourceEsIndexName, String sourceEsIndexType, String sourceEsFieldName, String sourceEsFieldValue, int batchSize, String filePath) {
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

        String sourcePath = filePath + "/source.json";//源文件
        String ipModelPath = filePath + "/ip_model.json";//根据ipstr匹配僵木控制事件
        String exploitModelPath = filePath + "/exploit_model.json";//匹配的漏洞
        String ipUnitModelPath = filePath + "/ipunit_model.json";//根据ipstr匹配ip单位
        String statisticalPath = filePath + "/exploit_statistical.txt";//统计结果

        //从数据源录入的字段数据
        String[] sourceFiedls = new String[]{"ip_str",
                "location.city.zh-CN",
                "product.vendor",
                "product.vendorcn",
                "device.primary_type",
                "device.secondary_type",
                "service",
                "has_exploit",
                "exploit.cvssbasescore",
                "exploit.exact",
                "exploit.uuid"};
        EsDataService esDataService = new EsDataService(sourceEsHost, sourceEsIndexName, sourceEsIndexType, sourceFiedls);
        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put(sourceEsFieldName, sourceEsFieldValue);
        esDataService.extractData(paramMap, batchSize, sourcePath);
        String nextTempPath=sourcePath;

        //往数据模型里面添加 运营商和僵木控制字段
        if (hasEvent) {
            //通过es检测ip是否存在僵木 蠕虫事件
            if ("ES".equalsIgnoreCase(eventType)) {
                EventESDataExtract eventESDataExtract = new EventESDataExtract();
                //模型抽取
                String[] modelFields = new String[]{"ip_operator", "event_desc", "key"};
                eventESDataExtract.extract(sourcePath, ipModelPath, modelFields);
                nextTempPath = ipModelPath;
            }
            //通过hdfs检测是否存在僵木 蠕虫事件
            else if ("HDFS".equalsIgnoreCase(eventType)) {
                String partPath = System.getProperty("user.dir").replace("\\", "/") + "/event/jiangxi/20180612160305";
                String checkerPath = partPath + "/part-00000";
                EventHDFSDataExtract ipModel = new EventHDFSDataExtract();
                ipModel.extract(sourcePath, checkerPath, ipModelPath);
                nextTempPath = ipModelPath;
            }
        }

        //往数据模型添加 recordFields 等字段
        if (hasExploit) {
            String[] recordFields = new String[]{"vulnerability.vulType",
                    "vulnerability.threadType",
                    "vulnerability.cnnvd",
                    "vulnerability.cve",
                    "CVSS.riskLevel",
                    "product.vendor"};
            ExploitDataExtract exploitDataExtract = new ExploitDataExtract(sourceEsHost, "vulnerability", "base", recordFields);
            exploitDataExtract.extract(nextTempPath, exploitModelPath);
            nextTempPath = exploitModelPath;
        }

        //根据ip地址查询ip地址单位信息
        if (hasIpUnit) {
            IPUnitDataExtract ipUnitDataExtract = new IPUnitDataExtract("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@localhost:11521:orcl", "smcs_jx", "smcs");
            ipUnitDataExtract.extract(nextTempPath, ipUnitModelPath);
            nextTempPath = ipUnitModelPath;
        }

        //不区分IOT ICS统计
        if (hasStatistical) {
            ExploitStatistical exploitStatistical = new ExploitStatistical(null, null,sourceEsFieldValue);
            exploitStatistical.statistical(nextTempPath, statisticalPath);
            //iot 物联统计
            ExploitStatistical IOTExploitStatistical = new ExploitStatistical("device.primary_type", "IOT",sourceEsFieldValue);
            IOTExploitStatistical.statistical(nextTempPath, statisticalPath);
            //ICS 工控统计
            ExploitStatistical ICSexploitStatistical = new ExploitStatistical("device.primary_type", "ICS",sourceEsFieldValue);
            ICSexploitStatistical.statistical(nextTempPath, statisticalPath);
        }

        //将统计出来的结果保存到oracle数据库中
        if (hasDb) {
            JDBCService jdbcService = new JDBCService();
            jdbcService.buildJSONData(nextTempPath, "ipchecker_" + hanYuPinyinString + "_" + dateFormat.format(new Date()), false);
        }
    }

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
    public void startSimpleTask(String sourceEsHost, String sourceEsIndexName, String sourceEsIndexType, String sourceEsFieldName, String sourceEsFieldValue, int batchSize, String filePath) {
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
        EventESDataExtract eventESDataExtract = new EventESDataExtract();
        //模型抽取
        String[] modelFields = new String[]{"ip_operator", "event_desc", "key"};
        eventESDataExtract.extract(sourcePath, modelPath, modelFields);

        // 数据统计 将结果数据输出到文本文件中
        EventStatistical.statistical(modelPath, statisticalPath);

        //将数据结果写入到数据库中
        JDBCService jdbcService = new JDBCService();
        jdbcService.buildJSONData(modelPath, "ipchecker_" + hanYuPinyinString + "_" + dateFormat.format(new Date()), false);
    }

    private boolean hasEvent;//是否存在僵木蠕虫事件
    private String eventType;//僵尸蠕虫事件类型 es hdfs
    private boolean hasExploit;//是否关联漏洞数据
    private boolean hasIpUnit;//是否关联ip单位信息
    private boolean hasStatistical;//是否统计数据
    private boolean hasDb;//是否保存数据到数据库

    public TaskService() {
        eventType = "es";
        hasEvent = true;
        hasExploit = true;
        hasIpUnit = true;
        hasStatistical = true;
        hasDb = true;
    }

    public TaskService(boolean hasEvent, String eventType, boolean hasExploit, boolean hasIpUnit, boolean hasStatistical, boolean hasDb) {
        this.hasEvent = hasEvent;
        this.hasExploit = hasExploit;
        this.hasIpUnit = hasIpUnit;
        this.hasStatistical = hasStatistical;
        this.hasDb = hasDb;
        this.eventType = eventType;
    }
}
