package com.tracker.hive.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 操作hive客户端示例
 * @author xiaorui.lu
 * 
 */
public class HiveJdbcClient {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://10.100.2.94:10000/default";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection conn = DriverManager.getConnection(url, "", "");
		Statement stmt = conn.createStatement();
		String tableName = "nexus";
		stmt.execute("drop table if exists " + tableName);
		stmt.execute("create table " + tableName + " (key int, value string)");
		System.out.println("Create table success!");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}

		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

}
