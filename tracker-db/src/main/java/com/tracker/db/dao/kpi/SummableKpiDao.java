package com.tracker.db.dao.kpi;

import java.util.List;
import java.util.Map;

import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;

/**
 * 文件名：SummableKpiDao
 * 创建人：jason.hua
 * 创建日期：2014-10-27 下午12:48:13
 * 功能描述：可累计指标数据接口
 */
public interface SummableKpiDao {
	//===========================================================更新kpi========================================================================

	/**
	 * 函数名：updateWebSiteKpi
	 * 功能描述：更新基于网站统计的可累加kpi
	 * @param row 
	 * @param kpi
	 */
	public void updateWebSiteKpi(String row,  WebSiteSummableKpi kpi);

	public void updateWebSiteKpi(Map<String, WebSiteSummableKpi> kpiMap);
	
	/**
	 * 函数名：updatePageKpi
	 * 功能描述：更新基于页面的可累加kpi
	 * @param row
	 * @param kpi
	 */
	public void updatePageKpi(String row,  PageSummableKpi kpi);

	public void updatePageKpi(Map<String, PageSummableKpi> kpiMap);
	
	/**
	 * 函数名：updateSearchKpi
	 * 功能描述：更新基于站内搜索的可累加kpi
	 * @param row
	 * @param kpi
	 */
	public void updateSearchKpi(String row, SearchSummableKpi kpi);

	public void updateSearchKpi(Map<String, SearchSummableKpi> kpiMap);
	
	
	//===========================================================获取kpi========================================================================
	/**
	 * 函数名：getWebSitePVKpi
	 * 功能描述：获取网站统计指定维度下的pv指标
	 * @param rowPrefixList
	 * @param fieldIndex
	 * @return
	 */
	public Map<String, Long> getWebSitePVKpi(List<String> rowPrefixList, Integer fieldIndex);

	/**
	 * 函数名：getWebSiteKpi
	 * 功能描述： 获取基于网站统计的可累加kpi值
	 * @param rowPrefixList
	 * @param fieldIndex
	 * @return
	 */
	public Map<String, WebSiteSummableKpi> getWebSiteKpi(List<String> rowPrefixList, Integer fieldIndex);

	/**
	 * 函数名：getWebSiteTopKpi
	 * 功能描述：获取可累加Top kpi值(外部来源、外部搜索引擎搜索词、系统环境)
	 * @param rowPrefixList
	 * @param fieldIndex
	 * @param topCount
	 * @return
	 */
	public Map<String, WebSiteSummableKpi> getWebSiteTopKpi(List<String> rowPrefixList, Integer fieldIndex, int topCount);
	
	/**
	 * 函数名：getWebSitePageKpi
	 * 功能描述：获取基于页面的可累加指标
	 * @param rowPrefixList
	 * @param fieldIndex
	 * @return
	 */
	public Map<String, PageSummableKpi> getWebSitePageKpi(List<String> rowPrefixList, Integer fieldIndex);

	/**
	 * 函数名：getSearchKpi
	 * 功能描述：获取基于站内搜索的可累加指标
	 * @param rowPrefix
	 * @param fieldIndex
	 * @return
	 */
	public Map<String, SearchSummableKpi> getSearchKpi(String rowPrefix, Integer fieldIndex);
}
