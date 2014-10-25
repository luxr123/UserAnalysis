package com.tracker.db.dao.siteSearch.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_search_top_value", defaultFamily = "stats")
public class TopSearchValueStats {
	public static final String SIGN_SE_TOP_SEARCH_VALUE = "se-top-val"; //基于搜索值

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int SEARCH_ENGINE_INDEX = 4;
	public static final int SEARCH_TYPE_INDEX = 5;
	public static final int SEARCH_CONDITION_INDEX = 6;
	public static final int SEARCH_COUNT_INDEX = 7;
	public static final int SEARCH_VALUE_INDEX = 8;

	@HBaseColumn(qualifier = "searchCount")
	public Long searchCount;

	
	public static String generateRow(String requiredRowPrefix, Long searchCount, String searchValue){
		Util.checkNull(searchCount);
		Util.checkNull(searchValue);
		return requiredRowPrefix + (Long.MAX_VALUE - searchCount) + RowUtil.ROW_SPLIT + searchValue;
	}
	
	public static String generateRequiredRowPrefix(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer searchCondType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		Util.checkNull(seId);
		Util.checkNull(searchCondType);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_SE_TOP_SEARCH_VALUE).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null? "":searchType).append(RowUtil.ROW_SPLIT);
		sb.append(searchCondType).append(RowUtil.ROW_SPLIT);
		
		return sb.toString();
	}
	
	public Long getSearchCount() {
		return searchCount;
	}

	public void setSearchCount(Long searchCount) {
		this.searchCount = searchCount;
	}

}
