package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.TopSearchRtStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储TopResponseTime的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class TopRtStatsStorage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(TopRtStatsStorage.class);

	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), TopSearchRtStats.class);
	private TopSearchRtStats stats = new TopSearchRtStats();

	public int evaluate(Integer dateType, Integer webId, String date, String ip, String cookieId, String userId, Integer userType,
			Long serverTime, Integer categoryId, Integer searchType, Integer responseTime, Long totalCount, Integer num,
			String condition) {
		try {
			//构造rowkey
			String rowKey = TopSearchRtStats.generateRow(webId, UDFUtils.getDateType(dateType), date, categoryId, responseTime, searchType, num);

			//对象赋值
			stats.setIp(ip);
			stats.setCookieId(cookieId);
			stats.setUserId(Integer.parseInt(userId));
			stats.setUserType(userType);
			stats.setSearchType(searchType);
			stats.setResponseTime(responseTime);
			stats.setServerTime(serverTime);
			stats.setTotalResultCount(totalCount);
			stats.setSearchValueStr(condition);

			//存储在hbase中
			dao.putObject(rowKey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to store TopRtStats", e);
			
			return 0;
		}
	}
}
