package com.tracker.storm.common;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.utils.Config.RichProperties;
import com.tracker.common.utils.ConfigExt;
import com.tracker.db.hbase.HbaseUtils;

public class StormConfig implements Serializable{
	private static final long serialVersionUID = 1810573835454763827L;
	public final static String ZOOKEEPER_NAME = "hbase.zookeeper.quorum";
	public final static String HBASE_POOL_SIZE = "hbase.pool.size";
	public final static String LOCAL_CACHE_PERIOD = "local.cache.period";
	public final static String LOCAL_CACHE_BATCH_SIZE = "local.cache.batch.size";
	public final static String IP_PATH_NAME = "ip.data.path";
	public final static String UNIVERSITY_PATH_NAME = "ip.university.path";

	
	/**
	 * HDFS_LOCATION,从系统变量中获取hdfs路径，hdfs://10.100.2.94
	 * COMMON_CONFIG, 从系统变量中获取配置文件路径，/tracker/resource/config.properties
	 */
	private String hdfsPath;
	private String commonConfigPath;
	private RichProperties prop;

	/**
	 * hbase
	 */
	private String zookeeper;
	private int hbasePoolSize;
	
	/**
	 * local cache 
	 */
	private int localCachePeriod; //second
	private int localCacheSize;

	/**
	 * ip
	 */
	private String ipDataPath;
	private String universityDataPath;
	
	public StormConfig(String hdfsPath, String commonConfigPath){
		this.hdfsPath = hdfsPath;
		this.commonConfigPath = commonConfigPath;
		prop = ConfigExt.getProperties(hdfsPath, commonConfigPath);

		/**
		 * hbase
		 */
		zookeeper = prop.getString(ZOOKEEPER_NAME);
		hbasePoolSize = prop.getInt(HBASE_POOL_SIZE, 1);
		
		/**
		 * local cache 
		 */
		localCachePeriod = prop.getInt(LOCAL_CACHE_PERIOD, 5); //second
		localCacheSize = prop.getInt(LOCAL_CACHE_BATCH_SIZE, 100);

		/**
		 * ip
		 */
		ipDataPath = prop.getString(IP_PATH_NAME);
		universityDataPath = prop.getString(UNIVERSITY_PATH_NAME);
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public String getCommonConfigPath() {
		return commonConfigPath;
	}

	public RichProperties getProp() {
		return prop;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	public HConnection getHbaseConnection() {
		return HbaseUtils.getHConnection(zookeeper, hbasePoolSize);
	}

	public int getLocalCachePeriod() {
		return localCachePeriod;
	}

	public int getLocalCacheSize() {
		return localCacheSize;
	}

	public String getIpDataPath() {
		return ipDataPath;
	}

	public String getUniversityDataPath() {
		return universityDataPath;
	}
	
}
