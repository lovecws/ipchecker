package com.surfilter.ipchecker.statistical.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * 获取到spark的连接
 */
public class SparkClient {

    private static String SPARK_MASTER = "local[2]";
    private static String HADOOP_URL = "hdfs://192.168.11.25:9000";

    private static JavaSparkContext sparkContext = null;

    public synchronized JavaSparkContext javaSparkContext() {
        if (sparkContext == null) {
            SparkConf conf = new SparkConf();
            conf.setAppName("ipCheckerSpark");
            String master = getMaster();
            conf.setMaster(master);
            conf.set("spark.streaming.receiver.writeAheadLogs.enable", "true");
            conf.set("spark.driver.allowMultipleContexts", "true");
            sparkContext = new JavaSparkContext(conf);
            if (!master.contains("local")) {
                sparkContext.addJar(hadoopAddress() + "/mumu/spark/jar/mumu-spark.jar");
            }
        }
        return sparkContext;
    }

    public synchronized SQLContext sqlContext() {
        String userDir = System.getProperty("user.dir");
        SparkSession sparkSession = SparkSession
                .builder()
                .master(getMaster())
                .appName("mumuSqlSpark")
                .config("spark.sql.warehouse.dir", userDir + File.separator + "spark-warehouse")
                .config("spark.driver.allowMultipleContexts", true)
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        return sqlContext;
    }

    public synchronized SQLContext hiveContext() {
        String userDir = System.getProperty("user.dir");
        SparkSession sparkSession = SparkSession
                .builder()
                .master(getMaster())
                .appName("mumuHiveSpark")
                .config("spark.sql.warehouse.dir", userDir + File.separator + "hive-warehouse")
                .config("spark.driver.allowMultipleContexts", true)
                .enableHiveSupport()
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        return sqlContext;
    }

    /**
     * 根据系统的配置 获取master
     *
     * @return
     */
    public String getMaster() {
        String spark_master = System.getenv("SPARK_MASTER");
        if (spark_master != null && !"".equals(spark_master)) {
            SPARK_MASTER = spark_master;
        }
        if (SPARK_MASTER.contains("yarn") && System.getenv("HADOOP_CONF_DIR") == null && System.getenv("YARN_CONF_DIR") == null) {
            String path = SparkClient.class.getResource("/hadoop").getPath();
            System.setProperty("HADOOP_CONF_DIR", path);
        }
        if (SPARK_MASTER.contains("mesos")) {
            System.setProperty("MESOS_NATIVE_JAVA_LIBRARY", "D:\\program\\mesos-1.5.0\\libmesos-1.5.0.so");
        }
        return SPARK_MASTER;
    }

    public String hadoopAddress() {
        String hadoop_url = System.getenv("HADOOP_URL");
        if (hadoop_url != null && !"".equals(HADOOP_URL)) {
            HADOOP_URL = hadoop_url;
        }
        return HADOOP_URL;
    }
}
