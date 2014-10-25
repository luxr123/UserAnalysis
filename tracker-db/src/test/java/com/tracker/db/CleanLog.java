package com.tracker.db;

import java.util.Properties;

import com.tracker.common.utils.ConfigExt;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.redis.JedisUtil;

public class CleanLog {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
//		String configFile = java.lang.System.getenv("COMMON_CONFIG");
//		Properties properties = ConfigExt.getProperties(hdfsLocation, configFile);
//		JedisUtil.getInstance(properties).KEYS.flushdb();
		
		// TODO Auto-generated method stub
//		HBaseDao ipIndexTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "ip_index");
//		ipIndexTable.deleteObjectByRowPrefix("1-", null);
//		
//		HBaseDao cookieIndexTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "cookie_index");
//		cookieIndexTable.deleteObjectByRowPrefix("1-", null);
//		
//		HBaseDao userIndexTable = new HBaseDao(HbaseUtils.getHCo  nnection("10.100.2.92,10.100.2.93"), "user_index");
//		userIndexTable.deleteObjectByRowPrefix("1-", null);
//		
//		HBaseDao logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "log_website");
//		logTable.deleteObjectByRowPrefix("9", null);
		
		HBaseDao logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "rt_kpi_summable");
		logTable.deleteObjectByRowPrefix("");
		
		logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "rt_kpi_unsummable_day");
		logTable.deleteObjectByRowPrefix("");
		
		logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "rt_search_top_res_time");
		logTable.deleteObjectByRowPrefix("");
		
		logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "rt_search_top_value");
		logTable.deleteObjectByRowPrefix("");
		
		logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "rt_search_top_ip");
		logTable.deleteObjectByRowPrefix("");
		
		logTable = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"), "website_user_session");
		logTable.deleteObjectByRowPrefix("");
	}

}
