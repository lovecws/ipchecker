package com.surfilter.ipchecker.statistical.spark;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.util.MapFieldUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkEventStatistical implements Serializable {

    private static final Logger log = Logger.getLogger(SparkEventStatistical.class);

    /**
     * 按照字段进行统计
     *
     * @param filePath  源文件路径
     * @param fieldName 字段名称 eventDesc、eventType、latestRecord.ip_operator
     * @param outPath
     */
    public void statistical(final String filePath, final String fieldName, final String outPath) {
        SparkClient sparkClient = new SparkClient();
        JavaSparkContext javaSparkContext = sparkClient.javaSparkContext();

        JavaRDD<String> javaRDD = javaSparkContext.textFile(filePath);
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
                if (line == null || "".equals(line)) {
                    return null;
                }
                List<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String, Integer>>();
                Map recordMap = JSON.parseObject(line, Map.class);
                String url = recordMap.keySet().toArray()[0].toString();
                List<Map> events = (List<Map>) recordMap.get(url);
                for (Map eventMap : events) {
                    String statisticalField = MapFieldUtil.getStatisticalField(eventMap, fieldName);
                    Tuple2<String, Integer> tuple2 = new Tuple2<>(statisticalField, 1);
                    tuples.add(tuple2);
                }
                return tuples.iterator();
            }
        });
        JavaPairRDD<String, Integer> reduceJavaPairRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reduceJavaPairRDD.persist(StorageLevel.MEMORY_ONLY());
        List<Tuple2<String, Integer>> collect = reduceJavaPairRDD.collect();
        for (Tuple2<String, Integer> tuple : collect) {
            log.info(tuple);
        }
        if (outPath != null && !"".equals(outPath)) {
            reduceJavaPairRDD.saveAsTextFile(outPath);
        }
    }


    /**
     * 按照字段进行统计
     *
     * @param filePath  源文件路径
     * @param fieldName 字段名称 eventDesc、eventType、latestRecord.ip_operator
     * @param outPath
     */
    public void statisticalFromModel(final String filePath, final String fieldName, final String outPath) {
        SparkClient sparkClient = new SparkClient();
        JavaSparkContext javaSparkContext = sparkClient.javaSparkContext();
        JavaRDD<String> javaRDD = javaSparkContext.textFile(filePath);
        javaRDD.persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] fields = line.split(",");
                if (fields == null || fields.length != 3) {
                    return null;
                }
                if(StringUtils.isEmpty(fields[1])){
                    fields[1]="";
                }
                if(StringUtils.isEmpty(fields[2])||"0".equals(fields[2])){
                    fields[2]="未知";
                }
                if("1".equals(fields[2])){
                    fields[2]="僵木控制";
                }
                String statisticalField = null;
                String[] fieldNames = fieldName.split(",");
                for (int i = 0; i < fieldNames.length; i++) {
                    if (statisticalField == null) {
                        statisticalField = fields[Integer.parseInt(fieldNames[i])];
                    } else {
                        statisticalField = statisticalField + "-" + fields[Integer.parseInt(fieldNames[i])];
                    }
                }
                return new Tuple2<>(statisticalField, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceJavaPairRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reduceJavaPairRDD.persist(StorageLevel.MEMORY_ONLY());
        List<Tuple2<String, Integer>> collect = reduceJavaPairRDD.collect();
        for (Tuple2<String, Integer> tuple : collect) {
            log.info(tuple);
        }
        if (outPath != null && !"".equals(outPath)) {
            reduceJavaPairRDD.saveAsTextFile(outPath);
        }
    }
}
