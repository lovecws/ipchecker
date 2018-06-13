/**
 * Project Name:application-business
 * File Name:JDBCConnector.java
 * Package Name:com.surfilter.dhp.is2.visitlogtodb
 * Date:2013年11月21日上午11:03:09
 *
*/

package com.surfilter.ipchecker.common.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

/**
 * ClassName:JDBCConnector <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2013年11月21日 上午11:03:09 <br/>
 * 
 * @author dengqw
 * @version
 * @since JDK 1.6
 * @see
 */
@SuppressWarnings("unused")
public class JDBCConnector {

	private static String driver;
	private static String url;
	private static String user;
	private static String password;
	private static int initialSize;
	private static int maxActive;
	private static int maxIdle;
	private static int maxWait;
	private static boolean testOnBorrow;
	private static boolean removeAbandoned;
	private static int removeAbandonedTimeout;
	private static boolean logAbandoned;
	private static String validationQuery;
	private static boolean testWhileIdle;

	private static DataSource dataSource;
	private static JDBCConnector jdbcConnector;
	private static Logger logger = Logger.getLogger(JDBCConnector.class);

	/**
	 * 1:oracle数据库，2:mysql数据库
	 */
	private static int databaseType = 1;

	private static void init() {
		try {
			if (jdbcConnector == null) {
				jdbcConnector = new JDBCConnector();
			}
			Properties properties =new Properties();
			try {
				properties.load(JDBCConnector.class.getResourceAsStream("/config.properties"));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			Field[] fileds = JDBCConnector.class.getDeclaredFields();
			@SuppressWarnings("unchecked")
			Class<String> clazz = (Class<String>) Class.forName("java.lang.String");
			for (Field field : fileds) {
				if (field.getType().equals(clazz)) {
					field.set(jdbcConnector, properties.getProperty("jdbc." + field.getName()));
				} else if (field.getType().equals(int.class)) {
					String value = properties.getProperty("jdbc." + field.getName());
					if (value != null) {
						field.set(jdbcConnector, Integer.valueOf(value));
					}
				} else if (field.getType().equals(boolean.class)) {
					String value = properties.getProperty("jdbc." + field.getName());
					if (value != null) {
						field.set(jdbcConnector, Boolean.valueOf(value));
					}
				}
			}
			Class.forName(driver);
			if (driver.toLowerCase().contains("oracle")) {
				databaseType = 1;
			} else if (driver.toLowerCase().contains("mysql")) {
				databaseType = 2;
			}

			Properties p = new Properties();
			p.put("driver", driver);
			p.put("url", url);
			p.put("username", user);
			p.put("password", password);
			p.put("initialSize", initialSize + "");
			p.put("maxActive", maxActive + "");
			// p.put("maxIdle", maxIdle+"");
			p.put("maxWait", maxWait + "");
			p.put("testOnBorrow", testOnBorrow + "");
			p.put("validationQuery", validationQuery);
			p.put("testWhileIdle", testWhileIdle + "");
			//p.put("removeAbandoned", String.valueOf(removeAbandoned));
			//p.put("removeAbandonedTimeout", String.valueOf(removeAbandoned));
			//p.put("logAbandoned", String.valueOf(removeAbandoned));

			DataSource dds = null;
			int count = 0;
			boolean successFlag = false;
			while (count < 3) {
				Connection conn = null;
				try {
					dds = (DruidDataSource) DruidDataSourceFactory.createDataSource(p);
					if (dds != null) {
						conn = dds.getConnection();
						if (conn != null) {
							successFlag = true;
						}
					}
				} catch (Exception e) {
					logger.error("数据库连接池创建失败,重试中....");
					e.printStackTrace();
				} finally {
					try {
						if (conn != null) {
							conn.close();
						}
					} catch (Exception e2) {
						logger.error(e2);
					}
				}
				if (successFlag) {
					break;
				}
				count++;
			}

			if (successFlag) {
				logger.info("数据库连接池创建成功");
			} else {
				logger.error("创建失败！");
			}
			dataSource = dds;

		} catch (ClassNotFoundException e) {
			logger.error("数据库驱动未加载!");
		} catch (IllegalArgumentException e) {
			logger.error("数据库连接创建失败!");
		} catch (IllegalAccessException e) {
			logger.error("数据库连接创建失败!");
		}
	}

	public static JDBCConnector getInstance() {
		if (jdbcConnector == null) {
			logger.info("get Conection>>>");
			// synchronized (jdbcConnector) {
			// if(jdbcConnector==null){
			long startTime = System.currentTimeMillis();
			init();
			logger.info("get Conection successfully,use time:" + (System.currentTimeMillis() - startTime));
			// }
			// }
		}
		return jdbcConnector;

	}

	public static synchronized Connection getConnection() throws SQLException{
		if(dataSource==null){
			init();
		}
		return dataSource.getConnection();
	}

	/**
	 * 
	 * update:执行sql语句，适用于insert update delete 操作 <br/>
	 * 
	 * @author dengqw
	 * @param sql
	 *            sql语句
	 * @return
	 * @since JDK 1.6
	 */
	public static int update(String sql) {
		int i = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			i = ps.executeUpdate();
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, null);
		}
		return i;
	}

	/**
	 * 
	 * update:执行sql语句，适用于insert update delete 操作，
	 * params支持简单的数据类型:Integer,Long,Double,Float,String,Date(sql),Timestamp(sql)
	 * 等<br/>
	 * 
	 * @author dengqw
	 * @param sql
	 *            sql语句
	 * @param params
	 *            sql语句的参数，用于替换?
	 * @return
	 * @since JDK 1.6
	 */
	public static int update(String sql, Object[] params) {
		int i = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			passValue(params, ps);
			i = ps.executeUpdate();
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, null);
		}
		return i;
	}

	/**
	 * 
	 * queryUniqueNotBean:查询单个没有封装成bean的对象 <br/>
	 * 
	 * @author dengqw
	 * @param sql
	 *            sql语句
	 * @param params
	 *            sql参数数组，没有传null
	 * @return
	 * @since JDK 1.6
	 */
	public static Object queryUniqueNotBean(String sql, Object[] params) {
		Object obj = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			passValue(params, ps);
			rs = ps.executeQuery();
			if (rs.next()) {
				obj = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, rs);
		}

		return obj;
	}

	/**
	 * 
	 * queryUnique:查询单个对象
	 * 
	 * @author dengqw
	 * @param clazz
	 *            返回对象的Class对象
	 * @param sql
	 *            sql语句
	 * @param params
	 *            sql语句的参数，用于替换?
	 * @param map
	 *            映射数据库与对象字段（只需要映射查询字段的关系）
	 * @return
	 * @since JDK 1.6
	 */
	public static Object queryUnique(Class<?> clazz, String sql, Object[] params, Map<String, String> map) {
		Object obj = null;
		map = correct(map);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			passValue(params, ps);
			rs = ps.executeQuery();
			if (rs.next()) {
				obj = reflectToObject(clazz, map, rs);
			}
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, rs);
		}

		return obj;
	}

	/**
	 * query:查询数据
	 * 
	 * @author dengqw
	 * @param clazz
	 *            返回对象的Class对象
	 * @param sql
	 *            sql语句
	 * @param params
	 *            sql语句的参数，用于替换?
	 * @param map
	 *            映射数据库与对象字段（只需要映射查询字段的关系）
	 * @return
	 * @since JDK 1.6
	 */
	public static List<?> query(Class<?> clazz, String sql, Object[] params, Map<String, String> map) {
		map = correct(map);
		List<Object> lstObj = new ArrayList<Object>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			passValue(params, ps);
			rs = ps.executeQuery();
			while (rs.next()) {
				Object obj = reflectToObject(clazz, map, rs);
				lstObj.add(obj);
			}
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, rs);
		}

		return lstObj;
	}

	/**
	 * query:查询数据
	 * 
	 * @author dengqw
	 * @param clazz
	 *            返回对象的Class对象
	 * @param sql
	 *            sql语句
	 * @param map
	 *            映射数据库与对象字段（只需要映射查询字段的关系）
	 * @return
	 * @since JDK 1.6
	 */
	public static List<?> query(Class<?> clazz, String sql, Map<String, String> map) {
		map = correct(map);
		List<Object> lstObj = new ArrayList<Object>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				Object obj = reflectToObject(clazz, map, rs);
				lstObj.add(obj);
			}
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, rs);
		}

		return lstObj;
	}

	/**
	 * 
	 * query:返回Map类型的查询结果 <br/>
	 * 
	 * @author dengqw
	 * @param sql
	 *            sql语句
	 * @param params
	 *            sql语句的参数，无参数传null
	 * @return
	 * @since JDK 1.6
	 */
	public static List<Map<Integer, Object>> query(String sql, Object[] params) {
		List<Map<Integer, Object>> lstMap = new ArrayList<Map<Integer, Object>>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			passValue(params, ps);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnSize = rsmd.getColumnCount();
			while (rs.next()) {
				Map<Integer, Object> map = new HashMap<Integer, Object>();
				for (int i = 0; i < columnSize; i++) {
					map.put(i, rs.getObject(i + 1));
				}
				lstMap.add(map);
			}
		} catch (Exception e) {
			logger.error("error", e);
		} finally {
			free(conn, ps, rs);
		}
		return lstMap;
	}

	/**
	 * 
	 * reflectToObject:根据结果集反射生成对象 <br/>
	 * 
	 * @author dengqw
	 * @param clazz
	 *            对象的Class
	 * @param map
	 *            数据库与对象字段的映射关系
	 * @param rs
	 *            结果集
	 * @return
	 * @throws Exception
	 * @since JDK 1.6
	 */
	private static Object reflectToObject(Class<?> clazz, Map<String, String> map, ResultSet rs) throws Exception {
		Object obj = clazz.newInstance();
		ResultSetMetaData rsmd = rs.getMetaData();
		int colCount = rsmd.getColumnCount();
		for (int i = 0; i < colCount; i++) {
			String label = rsmd.getColumnName(i + 1);
			String fieldName = map.get(label);
			Field field = clazz.getDeclaredField(fieldName);
			String methodName = "get" + field.getType().getSimpleName();
			if (methodName.equals("getInteger")) {
				methodName = "getInt";
			}
			Object value = ResultSet.class.getMethod(methodName, String.class).invoke(rs, label);
			String argMethodName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
			clazz.getDeclaredMethod(argMethodName, value.getClass()).invoke(obj, value);
			// field.set(obj, value);
		}
		return obj;
	}

	/**
	 * 
	 * correct:矫正数据库字段大写. <br/>
	 * 
	 * @author dengqw
	 * @param map
	 *            映射Map
	 * @return
	 * @since JDK 1.6
	 */
	private static Map<String, String> correct(Map<String, String> map) {
		Map<String, String> correctMap = new HashMap<String, String>();
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String value = map.get(key);
			correctMap.put(key.toUpperCase(), value);
		}
		return correctMap;
	}

	/**
	 * 
	 * passValue:sql语句赋值 <br/>
	 * 
	 * @author dengqw
	 * @param params
	 *            sql语句的参数，用于替换?
	 * @param ps
	 *            Statement对象
	 * @throws Exception
	 * @since JDK 1.6
	 */
	private static void passValue(Object[] params, PreparedStatement ps) throws Exception {
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				String paramType = params[i].getClass().getSimpleName();
				String methodName = "set" + paramType;
				if (methodName.equals("setInteger")) {
					methodName = "setInt";
				}
				Class<?> paramClass = params[i].getClass();
				if (paramClass.equals(Integer.class)) {
					paramClass = int.class;
				} else if (paramClass.equals(Long.class)) {
					paramClass = long.class;
				} else if (paramClass.equals(Short.class)) {
					paramClass = short.class;
				} else if (paramClass.equals(Float.class)) {
					paramClass = float.class;
				} else if (paramClass.equals(Double.class)) {
					paramClass = double.class;
				} else if (paramClass.equals(Byte.class)) {
					paramClass = byte.class;
				} else if (paramClass.equals(Boolean.class)) {
					paramClass = boolean.class;
				}
				Method method = ps.getClass().getDeclaredMethod(methodName, new Class[] { int.class, paramClass });
				method.setAccessible(true);
				method.invoke(ps, new Object[] { i + 1, params[i] });
			}
		}
	}

	/**
	 * 
	 * free:释放连接资源. <br/>
	 * 
	 * @author dengqw
	 * @param con
	 *            Connection对象
	 * @param st
	 *            Stamement对象
	 * @param rs
	 *            结果集对象
	 * @since JDK 1.6
	 */
	public static void free(Connection con, Statement st, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			logger.error("error", e);
		} finally {
			try {
				if (st != null) {
					st.close();
				}
			} catch (SQLException e) {
				logger.error("error", e);
			} finally {
				try {
					if (con != null) {
						con.close();
					}
				} catch (SQLException e) {
					logger.error("error", e);
				}
			}
		}
	}

	/**
	 * 
	 * free:释放连接资源. <br/>
	 * 
	 * @author dengqw
	 * @param con
	 *            Connection对象
	 * @param st
	 *            Stamement对象
	 * @since JDK 1.6
	 */
	public static void free(Connection con, Statement st) {
		try {
			if (st != null) {
				st.close();
			}
		} catch (SQLException e) {
			logger.error("error", e);
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				logger.error("error", e);
			}
		}
	}

	public static void closeConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException e) {
			logger.error("error", e);
		}
	}

	public static void closeResultSet(ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			logger.error("error", e);
		}
	}

	public static void closeStatement(Statement st) {
		try {
			if (st != null) {
				st.close();
			}
		} catch (SQLException e) {
			logger.error("error", e);
		}
	}

	public static int getDatabaseType() {
		return databaseType;
	}

	public static void setDatabaseType(int databaseType) {
		JDBCConnector.databaseType = databaseType;
	}

	public static void main(String[] args) {
		JDBCConnector.getInstance();
	}
}
