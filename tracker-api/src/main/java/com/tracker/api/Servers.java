package com.tracker.api;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.Config;
import com.tracker.common.utils.Config.RichProperties;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.redis.JedisUtil;

public class Servers {
	private static Logger logger = LoggerFactory.getLogger(Servers.class);
	
	//constants
	private static final String HBASE_ZKQ_KEY = "hbase.zookeeper.quorum";
	private static final String HBASE_POOL_SIZE = "hbase.pool.size";
	private static final String THRIFT_PORT_WEB_STATS = "web.stats.service.thrift.port";
	
	
	public static RichProperties prop = Config.getConfig("config.properties");
	//hbase
	public static String ZOOKEEPER = prop.getString(HBASE_ZKQ_KEY);
	public static HConnection hbaseConnection = HbaseUtils.getHConnection(ZOOKEEPER, prop.getInt(HBASE_POOL_SIZE, 1));
	//thrift
	public static int webStatsThriftPort = prop.getInt(THRIFT_PORT_WEB_STATS, 44444);
	
	//storm drpc 
	public static String STORM_DRPC_SERVER = prop.getString("storm.drpc.server");
	public static int STORM_DRPC_PORT = prop.getInt("storm.drpc.port");
	
	/**
	 * shutdown 
	 */
	public static void shutdown(){
		//hbase
		try {
			hbaseConnection.close();
		} catch (IOException e) {
			logger.error("can not close hbase connection", e);
		}
	}
	
	public static JedisUtil getJedisCache(){
		return JedisUtil.getInstance(prop);
	}
}
