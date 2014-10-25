package com.tracker.hbase.service.sitesearch;

/**
 * 文件名：DeleteDataForSiteSearch
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:33:40
 * 功能描述：针对某个站内搜索统计数据业务，删除其中的数据
 *
 */
public interface DeleteDataForSiteSearch {
	/**
	 * 删除基于时间的统计数据
	 */
	public void deleteSearchStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);
	
	/**
	 * 删除基于展示页码、搜索结果数、响应时间、搜索时段的统计数据
	 */
	public void deleteSearchResultStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer resultType);
	
	/**
	 * 删除基于搜索条件的统计数据
	 */
	public void deleteSearchConditionStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType);
}
