package com.tracker.api.service.sitesearch;

import java.util.List;
import java.util.Map;

import com.tracker.api.thrift.search.SearchStats;
import com.tracker.api.thrift.search.SearchStatsResult;
import com.tracker.api.thrift.search.TopResponseTimeResult;


/**
 * 数据服务接口（日、周、月、年）
 * 
 * @author jason.hua
 * 
 */
public interface SiteSearchService {
	/**
	 * 总搜索次数
	 */
	public long getTotalSearchCount(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);
	
	/**
	 * 获取基于时间的统计数据
	 */
	public Map<String, SearchStats> getSearchStatsForDates(Integer webId, Integer timeType, List<String> times, Integer seId, Integer searchType);
	
	/**
	 * 获取基于展示页码、搜索结果数、响应时间、搜索时段的统计数据
	 */
	public Map<String, SearchStats> getSearchResultStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer resultType);
	
	/**
	 * 获取基于搜索条件的统计数据
	 */
	public Map<String, SearchStats> getSearchConditionStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);

	/**
	 * 基于搜索页面
	 */
	public Map<String, SearchStats> getSearchPageStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);
	
	/**
	 * 获取基于搜索值的统计数据
	 */
	public SearchStatsResult getSearchValueStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer conditionType,
			Integer startIndex, Integer offset);
	
	/**
	 * 获取top最慢响应时间记录
	 */
	public TopResponseTimeResult getTopResponseTimeResult(Integer webId, Integer timeType, String time, Integer seId, Integer searchType,
			Integer startIndex, Integer offset);
	
	/**
	 * 获取top最慢响应时间记录
	 */
	public SearchStatsResult getTopIpResult(Integer webId, Integer timeType, String time, Integer seId, Integer searchType,
			Integer startIndex, Integer offset);
}
