package com.tracker.db.dao.siteSearch.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_search_kpi", defaultFamily = "stats")
public class SearchDateStats {
	public static final String SIGN_SE_DATE = "se-date"; //基于日期

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int SEARCH_ENGINE_INDEX = 4;
	public static final int SEARCH_TYPE_INDEX = 5;
	
	@HBaseColumn(qualifier = "searchCount")
	public Long searchCount;
	
	@HBaseColumn(qualifier = "totalSearchCost")
	public Long totalSearchCost;

	@HBaseColumn(qualifier = "uv")
	public Long uv;
	
	@HBaseColumn(qualifier = "ip_count")
	public Long ipCount;
	
	@HBaseColumn(qualifier = "maxSearchCost")
	public Long maxSearchCost;
	
	@HBaseColumn(qualifier = "pageTurningCount")
	public Long pageTurningCount;

	public static String generateRow(Integer webId, Integer timeType, String time, Integer seId, Integer searchType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		Util.checkNull(seId);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_SE_DATE).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null?"": searchType);
		return sb.toString();
	}
	
	public Long getSearchCount() {
		return searchCount;
	}

	public Long getUv() {
		return uv;
	}

	public void setUv(Long uv) {
		this.uv = uv;
	}

	public Long getIpCount() {
		return ipCount;
	}

	public void setIpCount(Long ipCount) {
		this.ipCount = ipCount;
	}

	public void setSearchCount(Long searchCount) {
		this.searchCount = searchCount;
	}
	
	public Long getTotalSearchCost() {
		return totalSearchCost;
	}

	public void setTotalSearchCost(Long totalSearchCost) {
		this.totalSearchCost = totalSearchCost;
	}
	
	public Long getMaxSearchCost() {
		return maxSearchCost;
	}

	public void setMaxSearchCost(Long maxSearchCost) {
		this.maxSearchCost = maxSearchCost;
	}

	public Long getPageTurningCount() {
		return pageTurningCount;
	}

	public void setPageTurningCount(Long pageTurningCount) {
		this.pageTurningCount = pageTurningCount;
	}

}
