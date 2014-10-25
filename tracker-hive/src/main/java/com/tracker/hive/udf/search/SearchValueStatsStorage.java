package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.TopSearchValueStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储有关按照搜索条件值的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class SearchValueStatsStorage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(SearchValueStatsStorage.class);

	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), TopSearchValueStats.class);
	private TopSearchValueStats stats = new TopSearchValueStats();

	public int evaluate(Integer dateType, Integer webId, String date, Integer categoryId, Integer searchType, Integer searchCondition,
			String searchValue, Long searchCount) {
		try {
			//构造rowkey
			String rowPrefix = TopSearchValueStats.generateRequiredRowPrefix(webId, UDFUtils.getDateType(dateType), date, categoryId, searchType, searchCondition);
			String rowkey = TopSearchValueStats.generateRow(rowPrefix, searchCount, searchValue);
			
			//对象赋值
			stats.setSearchCount(searchCount);

			// 存储在hbase中
			dao.putObject(rowkey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to store SearchValueStats", e);
			
			return 0;
		}
	}
}