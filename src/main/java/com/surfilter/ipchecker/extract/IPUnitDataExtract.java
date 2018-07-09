package com.surfilter.ipchecker.extract;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.surfilter.ipchecker.common.db.JDBCConnector;
import com.surfilter.ipchecker.util.IPUtil;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 根据ip地址信息 查找到ip的具体单位信息
 */
public class IPUnitDataExtract {

    public static final Logger log = Logger.getLogger(IPUnitDataExtract.class);

    public void extract(String sourcePath, String outputPath) {
        if (sourcePath == null || outputPath == null) {
            throw new IllegalArgumentException();
        }
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        String readLine = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(sourcePath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File(outputPath)));
            //读取源文件
            while ((readLine = bufferedReader.readLine()) != null) {
                Map<String, Object> sourceMap = JSON.parseObject(readLine, Map.class);
                Map<String, String> stringStringMap = query(sourceMap.get("ip_str").toString());
                sourceMap.putAll(stringStringMap);
                String jsonString = JSON.toJSONString(sourceMap,SerializerFeature.SortField);
                log.info(jsonString);
                bufferedWriter.write(jsonString);
                bufferedWriter.newLine();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
            log.error(e1);
        } finally {
            try {
                if (bufferedReader != null) bufferedReader.close();
                if (bufferedWriter != null) bufferedWriter.close();
            } catch (IOException e) {
            }
            JDBCConnector.clean();
        }
    }

    private Map<String, String> query(String ip_str) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        //将ip地址转化为long
        long ipToLong = IPUtil.ipToLong(ip_str);
        Map<String, String> sourceMap = new HashMap<String, String>();
        String QUERY_SQL = "SELECT * FROM IP_GJCX_BAXX WHERE QSIP<? AND ZZIP >? ORDER BY DWMC";
        try {
            connection = JDBCConnector.getConnection();
            preparedStatement = connection.prepareStatement(QUERY_SQL);
            preparedStatement.setLong(1, ipToLong);
            preparedStatement.setLong(2, ipToLong);
            resultSet = preparedStatement.executeQuery();

            String dwmc = "", xxdz = "", lxrxm = "", lxrdh = "", lxrdzyj = "";
            //只取第一条
            if (resultSet.next()) {
                dwmc = resultSet.getString("DWMC");
                xxdz = resultSet.getString("XXDZ");
                lxrxm = resultSet.getString("LXRXM");
                lxrdh = resultSet.getString("LXRDH");
                lxrdzyj = resultSet.getString("LXRDZYJ");
            }
            sourceMap.put("ip_dwmc", dwmc);
            sourceMap.put("ip_xxdz", xxdz);
            sourceMap.put("ip_lxrxm", lxrxm);
            sourceMap.put("ip_lxrdh", lxrdh);
            sourceMap.put("ip_lxrdzyj", lxrdzyj);
        } catch (SQLException e) {
            log.error(e);
        } finally {
            JDBCConnector.free(connection, preparedStatement, resultSet);
        }
        return sourceMap;
    }

    private String driver;
    private String url;
    private String user;
    private String password;

    public IPUnitDataExtract(String driver, String url, String user, String password) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;

        Properties properties = new Properties();
        properties.setProperty("jdbc.driver", this.driver);
        properties.setProperty("jdbc.url", this.url);
        properties.setProperty("jdbc.user", this.user);
        properties.setProperty("jdbc.password", this.password);
        properties.setProperty("jdbc.initialSize", "1");
        properties.setProperty("jdbc.maxActive", "5");
        properties.setProperty("jdbc.maxIdle", "1");
        properties.setProperty("jdbc.maxWait", "10000");
        properties.setProperty("jdbc.testOnBorrow", "false");
        properties.setProperty("jdbc.validationQuery", "select 1 from dual");
        properties.setProperty("jdbc.testWhileIdle", "true");
        properties.setProperty("jdbc.databaseType", "1");
        properties.setProperty("jdbc.removeAbandoned", "false");
        properties.setProperty("jdbc.removeAbandonedTimeout", "3000");
        properties.setProperty("jdbc.logAbandoned", "false");
        JDBCConnector.init(properties);
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
