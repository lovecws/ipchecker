package com.surfilter.ipchecker.statistical.spark;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.util.MapFieldUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
}
