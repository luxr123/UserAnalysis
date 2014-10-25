package com.tracker.hbase.service.website;

/**
 * 文件名：DeleteDataForWebSite
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午4:29:05
 * 功能描述：针对某个网站统计数据业务，删除其中的数据
 *
 */

public interface DeleteDataForWebSite {
	
	/**================================================网站首页================================================================ **/
	/**
	 * 基于网站用户类型的指标
	 */
	public void deleteWebUserStats(Integer webId, Integer timeType, String time);
	
	
	/**================================================趋势分析================================================================ **/
	/**
	 * 基于小时段的指标
	 */
	public void deleteWebStatsByHour(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 基于时间的指标(求一周数据总和)
	 */ 
	public void deleteWebStats(Integer webId, Integer timeType, String time, Integer visitorType);
	
	
	/**================================================来源分析================================================================ **/
	/**
	 * 获取基于访问来源类型的指标
	 */ 
	public void deleteWebStatsForRefType(Integer webId, Integer timeType, String time, Integer visitorType);
	
	
	/**================================================受访分析================================================================ **/
	/**
	 * 基于省份获取指标
	 */
	public void deleteWebStatsForProvince(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId);
	
	/**
	 * 基于城市获取指标
	 */
	public void deleteWebStatsForCity(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId, Integer provinceId);
	
	/**
	 * 获取基于访问页数指标
	 */
	public void deleteWebStatsForPage(Integer webId, Integer timeType, String time, Integer visitorType);

	/**
	 * 获取基于入口页的指标
	 */
	public void deleteWebStatsForEntryPage(Integer webId, Integer timeType, String time, Integer visitorType);
	
	/**
	 * 获取基于系统环境的指标
	 */
	public void deleteWebStatsForSysEnv(Integer webId, Integer timeType, String time, Integer sysType, Integer visitorType);
}
