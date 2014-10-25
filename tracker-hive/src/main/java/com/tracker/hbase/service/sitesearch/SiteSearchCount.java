package com.tracker.hbase.service.sitesearch;

/**
 * 文件名：SiteSearchCount
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:43:59
 * 功能描述：对站内搜索的数据进行周、月、年统计，并将统计结果存入Hbase
 * 
 */
public interface SiteSearchCount {
	/**
	 * 获取基于时间的统计数据
	 */
	public void countSearchStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);
	
	/**
	 * 获取基于展示页码、搜索结果数、响应时间、搜索时段的统计数据
	 */
	public void countSearchResultStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer resultType);
	
	/**
	 * 获取基于搜索条件的统计数据
	 */
	public void countSearchConditionStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);
}
