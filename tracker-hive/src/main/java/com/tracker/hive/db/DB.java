package com.tracker.hive.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * hive 数据仓库DB连接,关闭
 * @author xiaorui.lu
 * 
 */
public class DB {
	private static final Logger logger = LoggerFactory.getLogger(DB.class);
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://10.100.2.94:10000/default";

	/**
	 * 函数名：getHiveConnection
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 上午9:52:23
	 * 功能描述：获取hive数据仓库连接
	 * @return
	 */
	public static Connection getHiveConnection() {
		Connection conn = null;
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, "", "");
		} catch (ClassNotFoundException e) {
			logger.error("error to getHiveConnection, driverName:" + driverName, e);
			System.exit(1);
		} catch (SQLException e) {
			logger.error("error to getHiveConnection, url:" + url, e);
			System.exit(2);
		}
		return conn;
	}

	/**
	 * 
	 * 函数名：close
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 上午9:53:24
	 * 功能描述：关闭hive数据仓库连接
	 * @param conn
	 */
	public static void close(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			logger.error("error to close Connection", e);
		}
	}
}
