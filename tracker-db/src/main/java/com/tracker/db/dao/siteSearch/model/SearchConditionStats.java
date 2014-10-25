package com.tracker.db.dao.siteSearch.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_search_kpi", defaultFamily = "stats")
public class SearchConditionStats {
	public static final String SIGN_SE_CONDITION = "se-cond"; //基于搜索条件

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int SEARCH_ENGINE_INDEX = 4;
	public static final int SEARCH_TYPE_INDEX = 5;
	public static final int SEARCH_CONDITION_INDEX = 6;
	
	@HBaseColumn(qualifier = "searchCount")
	public Long searchCount;

	public Long getSearchCount() {
		return searchCount;
	}

	public void setSearchCount(Long searchCount) {
		this.searchCount = searchCount;
	}
	
	public static String generateRow(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer searchCondType){
		Util.checkNull(searchCondType);
		return generateRowPrefix(webId, timeType, time, seId, searchType) + searchCondType;
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time, Integer seId, Integer searchType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		Util.checkZeroValue(seId);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_SE_CONDITION).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null?"": searchType).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
}
