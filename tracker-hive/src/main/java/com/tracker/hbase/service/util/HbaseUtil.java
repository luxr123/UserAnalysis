package com.tracker.hbase.service.util;

import org.apache.hadoop.hbase.client.HConnection;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;

/**
 * 文件名：HbaseUtil
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:58:43
 * 功能描述：提供Hbase连接的获取以及关闭操作
 *
 */
public class HbaseUtil {
	/**
	 * 获取连接
	 * @return
	 */
	public static HConnection getHConnection() {
		return HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST);
	}
	
	/**
	 * 关闭连接
	 */
	public static void shutdownHConnection(){
		HbaseUtils.closeConnection();
	}
}
