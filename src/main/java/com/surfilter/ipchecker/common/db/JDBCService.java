package com.surfilter.ipchecker.common.db;

import com.alibaba.druid.util.StringUtils;
import com.sun.xml.internal.fastinfoset.tools.FI_DOM_Or_XML_DOM_SAX_SAXEvent;
import com.surfilter.ipchecker.entity.EventEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.common.netty.util.internal.StringUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * jdbc基础操作
 *
 * @author ganliang
 */
public class JDBCService {

    private static final Logger log = Logger.getLogger(JDBCService.class);

    /**
     * 创建表
     *
     * @param tableName 表名称
     */
    public void createTable(String tableName) {
        Connection connection = null;
        Statement statement = null;
        String CREATE_SQL = "CREATE TABLE " + tableName.toUpperCase() + "(ip varchar(32) not null,event_type varchar(32) not null,ip_operator varchar(32))";
        try {
            connection = JDBCConnector.getConnection();
            statement = connection.createStatement();
            statement.execute(CREATE_SQL);
            log.info("创建表[" + CREATE_SQL + "]操作成功");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCConnector.free(connection, statement);
        }
    }

    /**
     * 检测表是否存在
     *
     * @param tableName 表名称
     * @return
     */
    public boolean tableExists(String tableName) {
        Connection connection = null;
        Statement statement = null;
        long count = 0;
        String existsSql = "select count(*) from user_tables where table_name = '" + tableName.toUpperCase() + "'";
        try {
            connection = JDBCConnector.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(existsSql);
            if (resultSet.next()) {
                count = resultSet.getLong(1);
            }
            log.info("表检测[" + existsSql + "]操作成功,表查询结果:" + count);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCConnector.free(connection, statement);
        }
        return count > 0;
    }

    /**
     * 表删除
     *
     * @param tableName 表名称
     */
    public void dropTable(String tableName) {
        Connection connection = null;
        Statement statement = null;
        String DELETE_SQL = "DROP TABLE " + tableName.toUpperCase();
        try {
            connection = JDBCConnector.getConnection();
            statement = connection.createStatement();
            statement.execute(DELETE_SQL);
            log.info("表删除[" + DELETE_SQL + "]操作成功");
        } catch (SQLException e) {
            log.error(e);
        } finally {
            JDBCConnector.free(connection, statement);
        }
    }

    /**
     * 批量插入数据
     *
     * @param tableName  表名称
     * @param events     事件集合
     * @param batchCount 批次大小
     */
    public void batchInsertData(String tableName, List<EventEntity> events, int batchCount) {
        log.info("开始导入oracle数据.................................................");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        int current_index = 0;
        String INSERT_SQL = "INSERT INTO " + tableName + "(ip,event_type,ip_operator) values(?,?,?)";
        try {
            connection = JDBCConnector.getConnection();
            preparedStatement = connection.prepareStatement(INSERT_SQL);
            for (EventEntity event : events) {
                preparedStatement.setString(1, event.getIp());
                preparedStatement.setString(2, event.getEventType());
                preparedStatement.setString(3, event.getIpOperator());
                preparedStatement.addBatch();
                current_index++;
                //批量执行
                if (current_index >= batchCount) {
                    int[] executeBatchs = preparedStatement.executeBatch();
                    current_index = 0;
                    log.info("批量导入数据[" + INSERT_SQL + "],操作结果" + Arrays.toString(executeBatchs));
                }
            }
            if(current_index>0){
                int[] executeBatchs = preparedStatement.executeBatch();
                log.info("最后一批导入数据[" + INSERT_SQL + "],操作结果" + Arrays.toString(executeBatchs));
            }
            connection.commit();
        } catch (SQLException e) {
            log.error(e);
            try {
                connection.rollback();
            } catch (SQLException e1) {
                log.error(e);
            }
        } finally {
            JDBCConnector.free(connection, preparedStatement);
            log.info("结束导入oracle数据.................................................");
        }
    }

    /**
     * @param filePath 模型文件名称
     * @param tableName 表名称
     * @param parseFields 解析的字段数组
     * @param operator 模型分隔符
     * @param dump 是否去除重复ip数据
     */
    public void buildData(String filePath, String tableName, String[] parseFields, String operator,boolean dump) {
        if (filePath == null || tableName == null) {
            throw new IllegalArgumentException();
        }
        if (parseFields == null) {
            parseFields = new String[]{"ip", "ipOperator", "eventType"};
        }
        if (parseFields != null && parseFields.length != 3) {
            throw new IllegalArgumentException();
        }

        BufferedReader bufferedReader = null;
        String readLine = null;
        List<EventEntity> events = new ArrayList<EventEntity>();
        Map<String, EventEntity> DUMP_IP_MAP = new HashMap<String, EventEntity>();
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
            while ((readLine = bufferedReader.readLine()) != null) {
                if (readLine == null || "".equals(readLine)) {
                    continue;
                }
                String[] eventFields = readLine.split(operator);
                EventEntity eventEntity = new EventEntity();
                for (int i = 0; i < parseFields.length; i++) {
                    String fieldName = parseFields[i];
                    String fieldValue = eventFields[i];
                    writeFieldValue(eventEntity, fieldName, fieldValue);
                }
               //是否去除重复
                if(dump){
                    String ip = eventEntity.getIp();
                    String eventType = eventEntity.getEventType();
                    String ipOperator = eventEntity.getIpOperator();
                    EventEntity entity = DUMP_IP_MAP.get(ip+"-"+eventType+"-"+ipOperator);
                    if (entity == null) {
                        events.add(eventEntity);
                        DUMP_IP_MAP.put(ip, eventEntity);
                    }
                }else{
                    events.add(eventEntity);
                }
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                }
            }
        }
        for (EventEntity eventEntity : events) {
            log.info(eventEntity);
        }
        log.info("total records " + events.size());

        //创建表
        createTable(tableName);
        //将数据导出到数据库中
        batchInsertData(tableName, events, 2000);
    }

    private void writeFieldValue(EventEntity eventEntity, String fieldName, String fieldValue) {
        Field[] declaredFields = eventEntity.getClass().getDeclaredFields();
        try {
            for (Field field : declaredFields) {
                field.setAccessible(true);
                if (field.getName().equalsIgnoreCase(fieldName)) {
                    if ("eventType".equalsIgnoreCase(fieldName) && "0".equalsIgnoreCase(fieldValue)) {
                        fieldValue = "未知";
                    }
                    if ("eventType".equalsIgnoreCase(fieldName) && "1".equalsIgnoreCase(fieldValue)) {
                        fieldValue = "僵木控制";
                    }
                    if ("ipOperator".equalsIgnoreCase(fieldName) && "".equalsIgnoreCase(fieldValue)) {
                        fieldValue = "未知";
                    }
                    field.set(eventEntity, fieldValue);
                }
            }
        } catch (IllegalAccessException e) {
            log.error(e);
        }
    }

    public List<Map<String, String>> query(String tableName, String queryField) {
        if (tableName == null || queryField == null || "".equalsIgnoreCase(queryField)) {
            throw new IllegalArgumentException();
        }
        Connection connection = null;
        Statement statement = null;
        String QUERY_SQL = null;
        switch (queryField) {
            case "event_type":
                QUERY_SQL = "select event_type,count(ip) ipcount from " + tableName + " group by event_type order by ipcount asc";
                break;
            case "ip_operator":
                QUERY_SQL = "select ip_operator,count(ip) ipcount from " + tableName + " group by ip_operator order by ipcount asc";
                break;
            case "event_type,ip_operator":
                QUERY_SQL = "select event_type,ip_operator,count(ip) ipcount from " + tableName + " group by event_type,ip_operator order by event_type asc,ipcount asc";
                break;
            case "ip_operator,event_type":
                QUERY_SQL = "select ip_operator,event_type,count(ip) ipcount from " + tableName + " group by ip_operator,event_type order by ip_operator asc,ipcount asc";
                break;
        }
        List<Map<String, String>> results = new ArrayList<Map<String, String>>();
        try {
            connection = JDBCConnector.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(QUERY_SQL);
            while (resultSet.next()) {
                Map<String, String> result = new HashMap<String, String>();
                int columnCount = resultSet.getMetaData().getColumnCount();
                for (int i =1; i <= columnCount; i++) {
                    String columnName = resultSet.getMetaData().getColumnName(i);
                    String columnValue = resultSet.getString(i);
                    result.put(columnName,columnValue);
                }
                results.add(result);
            }
        } catch (SQLException e) {
            log.error(e);
        } finally {
            JDBCConnector.free(connection, statement);
        }
        return results;
    }
}
