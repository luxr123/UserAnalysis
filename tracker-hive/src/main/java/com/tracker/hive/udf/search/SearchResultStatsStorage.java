package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.SearchResultStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储有关按照搜索结果值的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class SearchResultStatsStorage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(SearchResultStatsStorage.class);

	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), SearchResultStats.class);
	private SearchResultStats stats = new SearchResultStats();

	public int evaluate(Integer dateType, Integer webId, String date, Integer categoryId, Integer searchType, String searchPage,
			Integer searchShowType, Integer resultType, String typeValue, Long searchCount, Long totalSearchCost) {
		try {
			//构造rowkey
			String rowPrefix = SearchResultStats.generateRequiredRowPrefix(webId, UDFUtils.getDateType(dateType), date, categoryId, searchType, resultType);
			String rowKey = SearchResultStats.generateRow(rowPrefix, searchPage, searchShowType, typeValue);
			
			//对象赋值
			stats.setSearchCount(searchCount);
			stats.setTotalSearchCost(totalSearchCost);

			// 存储在hbase中
			dao.putObject(rowKey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to store SearchResultStats", e);
			
			return 0;
		}
	}
}