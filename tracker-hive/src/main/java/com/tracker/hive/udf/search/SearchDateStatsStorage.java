package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.SearchDateStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储有关按照搜索日期的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class SearchDateStatsStorage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(SearchDateStatsStorage.class);
	
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), SearchDateStats.class);
	private SearchDateStats stats = new SearchDateStats();

	public int evaluate(Integer dateType, Integer webId, String date, Integer categoryId, Integer searchType, Integer pageId,
			Integer searchShowType, Long uv, Long ip, Long searchCount, Long totalSearchCost, Long maxSearchCost) {
		try {
			//构造rowkey
			String rowKey = SearchDateStats.generateRow(webId, UDFUtils.getDateType(dateType), date, categoryId, searchType);
			//对象赋值
			stats.setUv(uv);
			stats.setIpCount(ip);
			stats.setSearchCount(searchCount);
			stats.setTotalSearchCost(totalSearchCost);
			stats.setMaxSearchCost(maxSearchCost);
			//存储到hbase中
			dao.putObject(rowKey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to store SearchDateStats", e);
			
			return 0;
		}
	}
}
