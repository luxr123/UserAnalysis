package com.tracker.db.hbase;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseUtils {
	private static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
	
	private static HConnection hbaseConnection = null;
	/**
	 * create hbase connection and Use {@link HConnection#getTable(String)} instead of {@link HTablePool#getTable(String)}
	 * see {@link HConnectionManager#createConnection(Configuration)}
	 * @param hbaseZookeeperQuorum
	 * @param pool
	 * @return
	 */
	private static HConnection createHConnection(String hbaseZookeeperQuorum, ExecutorService pool){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
		HConnection connection = null;
		try {
			connection = HConnectionManager.createConnection(conf, pool);
		} catch (IOException e) {
			logger.error("can not create hbase connection", e);
		}
		return connection;
	}
	
	private static HConnection createHConnection(String hbaseZookeeperQuorum){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
		HConnection connection = null;
		try {
			connection = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			logger.error("can not create hbase connection", e);
		}
		return connection;
	}
	
	public static HBaseAdmin getHBaseAdmin(String hbaseZookeeperQuorum){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			logger.error("can not create HBaseAdmin", e);
		} catch (ZooKeeperConnectionException e) {
			logger.error("can not create HBaseAdmin", e);
		} catch (IOException e) {
			logger.error("can not create HBaseAdmin", e);
		}catch(Exception e){
			logger.error("error while creating hbaseAdming");
		}
		return admin;
	}
	
	public static HConnection getHConnection(String hbaseZookeeperQuorum){
		if(hbaseConnection == null){
			synchronized (HbaseUtils.class) {
				if(hbaseConnection == null){
					hbaseConnection = HbaseUtils.createHConnection(hbaseZookeeperQuorum);
				}
			}
		}
		return hbaseConnection;
	}
	
	public static HConnection getHConnection(String hbaseZookeeperQuorum, int poolsize){
		if(hbaseConnection == null){
			synchronized (HbaseUtils.class) {
				if(hbaseConnection == null){
					hbaseConnection = HbaseUtils.createHConnection(hbaseZookeeperQuorum, Executors.newFixedThreadPool(poolsize));
				}
			}
		}
		return hbaseConnection;
	}
	
	public static void closeConnection(){
		try {
			if(hbaseConnection != null){
				hbaseConnection.close();
				hbaseConnection = null;
			}
		} catch (IOException e) {
			logger.error("error to close HConnection", e);
		}
	}
	
	public static void closeConnection(HConnection hbaseConnection){
		try {
			if(hbaseConnection != null){
				hbaseConnection.close();
				hbaseConnection = null;
			}
		} catch (IOException e) {
			logger.error("error to close HConnection", e);
		}
	}
}
