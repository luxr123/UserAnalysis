package com.tracker.hbase.service.sitesearch;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.SearchConditionStats;
import com.tracker.db.dao.siteSearch.model.SearchDateStats;
import com.tracker.db.dao.siteSearch.model.SearchResultStats;
import com.tracker.hbase.service.util.HbaseUtil;

/**
 * 文件名：DeleteDataForSiteSearchImpl
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:37:00
 * 功能描述：DeleteDataForSiteSearch接口实现类
 *
 */
public class DeleteDataForSiteSearchImpl implements DeleteDataForSiteSearch {
	private static HBaseDao searchDataDao = new HBaseDao(HbaseUtil.getHConnection(), SearchDateStats.class);
	private static HBaseDao searchResultDao = new HBaseDao(HbaseUtil.getHConnection(), SearchResultStats.class);
    private static HBaseDao searchConditionDao = new HBaseDao(HbaseUtil.getHConnection(), SearchConditionStats.class);
	  
 
	@Override
	public void deleteSearchStats(Integer webId, Integer timeType, String time,
			Integer seId, Integer searchType) {
		searchDataDao.deleteObjectByRowPrefix(SearchDateStats.generateRow(webId, timeType, time, seId, searchType));
		
	}

	@Override
	public void deleteSearchResultStats(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType, Integer resultType) {
		searchResultDao.deleteObjectByRowPrefix(SearchResultStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, resultType));
		
	}

	@Override
	public void deleteSearchConditionStats(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType) {
		searchConditionDao.deleteObjectByRowPrefix(SearchConditionStats.generateRowPrefix(webId, timeType, time, seId, searchType));
	}
}
