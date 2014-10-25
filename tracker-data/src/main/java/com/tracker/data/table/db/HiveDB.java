
package com.tracker.data.table.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**     
 * Simple to Introduction
 * @ProjectName:  [tracker-hive]
 * @Package:      [com.tracker.hive]
 * @ClassName:    [DB]
 * @Description:  [一句话描述该类的功能]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年5月21日 下午3:50:03]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年5月21日 下午3:50:03]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *      
 */
public class HiveDB {
	private static Logger logger = LoggerFactory.getLogger(HiveDB.class);
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://10.100.2.94:10000/default";
	
	public static Connection getHiveConnection(){
		Connection conn = null;
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, "", "");
		} catch (ClassNotFoundException e) {
			logger.error("error to get Connection", e);
		} catch (SQLException e) {
			logger.error("error to get Connection", e);
		}
		return conn;
	}
	
	/**
	 * 创建table
	 * @param conn
	 * @param tableName
	 * @param sql
	 * @throws SQLException
	 */
	public static void createTable(Connection conn, String tableName, String sql) throws SQLException{
		Statement stmt = conn.createStatement();
		ResultSet res = stmt.executeQuery("show tables '" + tableName + "'");
		if (res.next()) {
			HiveDB.executeSql(conn, "drop table " + tableName);
		}
		stmt.execute(sql);
		close(res, stmt);
	}
	
	public static void executeSql(Connection conn, String sql) throws SQLException{
		Statement stmt = conn.createStatement();
		stmt.execute(sql);
		close(null, stmt);
	}
	
	public static void close(ResultSet res, Statement stmt) throws SQLException{
		if(res != null)
			res.close();
		if(stmt != null)
			stmt.close();
	}
	
	/**
	 * relase jdbc connection
	 * @param conn
	 * @throws SQLException
	 */
	public static void relaseConnection(Connection conn) throws SQLException{
		if(conn != null){
			conn.close();
		}
	}
}

