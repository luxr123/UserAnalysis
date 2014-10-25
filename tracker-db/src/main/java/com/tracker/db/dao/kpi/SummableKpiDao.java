package com.tracker.db.dao.kpi;

import java.util.List;
import java.util.Map;

import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;

/**
 * 可累计指标统计
 * @author jason.hua
 *
 */
public interface SummableKpiDao {
	//===========================================================更新kpi========================================================================

	/**
	 * 更新可累加kpi(访问次数、访问时间、访问页数、pv数、跳出次数)
	 */
	public void updateWebSiteKpi(String row,  WebSiteSummableKpi kpi);

	public void updateWebSiteKpi(Map<String, WebSiteSummableKpi> kpiMap);
	
	/**
	 * 更新可累加kpi(访问次数、访问时间、访问页数、pv数、跳出次数)
	 */
	public void updatePageKpi(String row,  PageSummableKpi kpi);

	public void updatePageKpi(Map<String, PageSummableKpi> kpiMap);
	
	
	/**
	 * 更新可累加kpi(访问次数、访问时间、访问页数、pv数、跳出次数)
	 */
	public void updateSearchKpi(String row, SearchSummableKpi kpi);

	public void updateSearchKpi(Map<String, SearchSummableKpi> kpiMap);
	
	
	//===========================================================获取kpi========================================================================
	/**
	 * 获取指定维度下的pv指标
	 */
	public Map<String, Long> getWebSitePVKpi(List<String> rowPrefixList, Integer fieldIndex);

	/**
	 * 获取可累加kpi值(访问次数、访问时间、访问页数、pv数、跳出次数)
	 */
	public Map<String, WebSiteSummableKpi> getWebSiteKpi(List<String> rowPrefixList, Integer fieldIndex);

	/**
	 * 获取可累加Top kpi值(外部来源、外部搜索引擎搜索词、系统环境)
	 */
	public Map<String, WebSiteSummableKpi> getWebSiteTopKpi(List<String> rowPrefixList, Integer fieldIndex, int topCount);
	
	/**
	 * 获取可累加受访页kpi(入口页次数、出口页次数、贡献下游次数、停留时长)
	 */
	public Map<String, PageSummableKpi> getWebSitePageKpi(List<String> rowPrefixList, Integer fieldIndex);

	/**
	 * 获取可累加kpi值(搜索次数、耗时、最大耗时)
	 */
	public Map<String, SearchSummableKpi> getSearchKpi(String rowPrefix, Integer fieldIndex);
}
