package com.tracker.db.dao.kpi;

import java.util.List;
import java.util.Map;

/**
 * 文件名：UnSummableKpiDao
 * 创建人：jason.hua
 * 创建日期：2014-10-27 下午12:51:37
 * 功能描述：ip和UV统计数据接口
 *
 */
public interface UnSummableKpiDao {
	/**
	 * 函数名：updateUnSummableKpi
	 * 功能描述：更新不可累加kpi（ip和uv）
	 * @param rowList
	 */
	public void updateUnSummableKpi(List<String> rowList);
	
	/**
	 * 函数名：getWebSiteUnSummableKpi
	 * 功能描述：获取网站统计不可累加kpi（ip和uv）
	 * @param kpi
	 * @param date
	 * @param webId
	 * @param sign
	 * @param visitorType
	 * @param fields
	 * @return
	 */
	public Map<String, Long> getWebSiteUnSummableKpi(String kpi, String date, String webId, String sign, Integer visitorType, List<String> fields);
	

	/**
	 * 函数名：getSearchUnSummableKpiForDate
	 * 功能描述：获取不可累加kpi(ip数、搜索人数)
	 * @param kpi
	 * @param date
	 * @param webId
	 * @param sign
	 * @param seId
	 * @param searchType
	 * @return
	 */
	public Long getSearchUnSummableKpiForDate(String kpi, String date, String webId, String sign, Integer seId, Integer searchType);
	
}
