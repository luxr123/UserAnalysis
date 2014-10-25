package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.TopSearchIpStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储TopIp的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class TopIpStatsStorage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(TopIpStatsStorage.class);

	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), TopSearchIpStats.class);
	private TopSearchIpStats stats = new TopSearchIpStats();


	public int evaluate(Integer dateType, Integer webId, String date, Integer categoryId, Integer searchType, String ip, Long searchCount, Integer num) {
		try {
			//构造rowkey
			String rowkey=  TopSearchIpStats.generateRow(webId, UDFUtils.getDateType(dateType), date, categoryId, searchType, num);
			
			//对象赋值
			stats.setIp(ip);
			stats.setSearchCount(searchCount);

			// 存储在hbase中
			dao.putObject(rowkey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to store TopIpStats", e);
			
			return 0;
		}
	}
}
