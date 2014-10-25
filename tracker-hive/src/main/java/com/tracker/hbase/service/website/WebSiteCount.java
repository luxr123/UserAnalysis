package com.tracker.hbase.service.website;

/**
 * 文件名：WebSiteCount
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午4:47:43
 * 功能描述：对网站统计的数据进行周、月、年统计，并将统计结果存入Hbase
 *
 */
public interface WebSiteCount {
	
	/**================================================网站首页================================================================ **/
	/**
	 * 基于网站用户类型的指标
	 */
	public void countWebUserStats(Integer webId, Integer timeType, String time);
	
	
	/**================================================趋势分析================================================================ **/
	/**
	 * 基于小时段的指标
	 */
	public void countWebStatsByHour(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 基于时间的指标(求一周数据总和)
	 */ 
	public void countWebStats(Integer webId, Integer timeType, String time, Integer visitorType);
	
	
	/**================================================来源分析================================================================ **/
	/**
	 * 获取基于访问来源类型的指标
	 */ 
	public void countWebStatsForRefType(Integer webId, Integer timeType, String time, Integer visitorType);
	
	
	/**================================================受访分析================================================================ **/
	/**
	 * 基于省份获取指标
	 */
	public void countWebStatsForProvince(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId);
	
	/**
	 * 基于城市获取指标
	 */
	public void countWebStatsForCity(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId, Integer provinceId);
	
	/**
	 * 获取基于访问页数指标
	 */
	public void countWebStatsForPage(Integer webId, Integer timeType, String time, Integer visitorType);

	/**
	 * 获取基于入口页的指标
	 */
	public void countWebStatsForEntryPage(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 获取基于系统环境的指标
	 */
	public void countWebStatsForSysEnv(Integer webId, Integer timeType, String time, Integer sysType, Integer visitorType);
}
