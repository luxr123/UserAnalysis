package com.tracker.db.dao.siteSearch;

import java.util.List;

import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult.ResponseTimeRecord;
import com.tracker.db.dao.siteSearch.entity.SearchTopResult;
import com.tracker.db.dao.siteSearch.entity.SearchValueParam;

/**
 * top统计
 * @author jason
 *
 */
public interface SearchRTTopDao {
	/**
	 * =============================Top最慢响应时间记录====================================================
	*/
	public void updateMaxRTRecord(String date, String webId, Integer seId, Integer searchType, ResponseTimeRecord record);
	
	public SearchTopResTimeResult getMaxRTRecord(String date, String webId, Integer seId, Integer searchType, int startIndex, int offset);
	
	/**
	 * =============================Top搜索次数最多Ip的记录====================================================
	 */
	public void updateMostSearchForIp(String date, String webId, Integer seId, Integer searchType, String ip, long searchCount);

	public SearchTopResult getMostSearchForIp(String date, String webId, Integer seId, Integer searchType, int startIndex, int offset);
	
	/**
	 * =============================Top搜索次数条件值====================================================
	 */
	public void updateMostForSearchValue(String date, String webId, Integer seId, Integer searchType, List<SearchValueParam> params);
	
	public SearchTopResult getMostForSearchValue(String date, String webId, Integer seId, Integer searchType, Integer searchConType, int startIndex, int offset);
	
}
