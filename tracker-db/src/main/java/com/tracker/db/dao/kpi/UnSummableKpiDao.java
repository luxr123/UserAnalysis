package com.tracker.db.dao.kpi;

import java.util.List;
import java.util.Map;

/**
 * ip和UV统计
 * @author jason.hua
 *
 */
public interface UnSummableKpiDao {
	/**
	 * 更新不可累加kpi（ip和uv）
	 */
	public void updateUnSummableKpi(List<String> rowList);
	
	/**
	 * 获取网站统计不可累加kpi（ip和uv）
	 */
	public Map<String, Long> getWebSiteUnSummableKpi(String kpi, String date, String webId, String sign, Integer visitorType, List<String> fields);
	

	/**
	 * 获取不可累加kpi(ip数、搜索人数)
	 */
	public Long getSearchUnSummableKpiForDate(String kpi, String date, String webId, String sign, Integer seId, Integer searchType);
	
}
