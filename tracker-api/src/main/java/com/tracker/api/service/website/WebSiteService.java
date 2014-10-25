package com.tracker.api.service.website;

import java.util.List;
import java.util.Map;

import com.tracker.api.thrift.web.EntryPageStats;
import com.tracker.api.thrift.web.PageStats;
import com.tracker.api.thrift.web.WebStats;


/**
 * 网站统计数据服务
 * @author jason.hua
 */
public interface WebSiteService {
	
	/**================================================网站首页================================================================ **/
	/**
	 * 获取基于网站用户类型的统计指标
	 */
	public Map<String, WebStats> getWebUserStats(Integer webId, Integer timeType, String time);
	
	
	/**================================================趋势分析================================================================ **/
	/**
	 * 获取基于小时段的统计指标
	 */
	public Map<String, WebStats> getWebStatsByHour(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 获取基于时间的统计指标
	 */ 
	public Map<String, WebStats> getWebStatsByDates(Integer webId, Integer timeType, List<String> times, Integer visitorType);
	
	
	/**================================================来源分析================================================================ **/
	/**
	 * 获取基于访问来源类型的统计指标
	 */ 
	public Map<String, WebStats> getWebStatsForRefType(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 获取基于搜索引擎来源、外部链接统计数据
	 */
	public Map<String, WebStats> getWebStatsForRefDomain(Integer webId, Integer timeType, String time, Integer refType, Integer visitorType, Integer topNum);
	
	/**
	 * 基于搜索词获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForKeyword(Integer webId, Integer timeType, String time, String seDomain, Integer visitorType, Integer topNum);
	
	/**================================================受访分析================================================================ **/
	/**
	 * 基于省份获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForProvince(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId);
	
	/**
	 * 基于城市获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForCity(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId, Integer provinceId);
	
	/**
	 * 获取基于访问页数统计指标
	 */
	public Map<String, PageStats> getWebStatsForPage(Integer webId, Integer timeType, String time, Integer visitorType);

	/**
	 * 获取基于入口页的统计指标
	 */
	public Map<String, EntryPageStats> getWebStatsForEntryPage(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 获取基于系统环境的统计指标
	 */
	public Map<String, WebStats> getWebStatsForSysEnv(Integer webId, Integer timeType, String time, Integer sysType, Integer visitorType, Integer topNum) ;
}
