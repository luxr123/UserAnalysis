package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.SearchConditionStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储有关搜索条件的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class SearchConditionStatsStorage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(SearchConditionStatsStorage.class);

	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), SearchConditionStats.class);
	private SearchConditionStats stats = new SearchConditionStats();

	
	public int evaluate(Integer dateType, Integer webId, String date, Integer categoryId, Integer searchType, Integer searchShowType,
			Integer searchCondition, Long searchCount) {
		try {
			//构造rowkey
			String rowkey = SearchConditionStats.generateRow(webId, UDFUtils.getDateType(dateType), date, categoryId, searchType, searchCondition);
			//对象赋值
			stats.setSearchCount(searchCount);
			// 存储在hbase中
			dao.putObject(rowkey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to store SearchConditionStats", e);
			
			return 0;
		}
	}
}