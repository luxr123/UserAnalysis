package com.tracker.db.dao.siteSearch.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_search_kpi", defaultFamily = "stats")
public class SearchResultStats {
	public static final String SIGN_SE_RESULT = "se-result"; //基于结果类型

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int SEARCH_ENGINE_INDEX = 4;
	public static final int SEARCH_TYPE_INDEX = 5;
	public static final int RESULT_TYPE_INDEX = 6;
	public static final int SEARCH_PAGE_INDEX = 7;
	public static final int SEARCH_SHOW_TYPE = 8;
	public static final int TYPE_VALUE_INDEX = 9;
	
	@HBaseColumn(qualifier = "searchCount")
	public Long searchCount;
	
	@HBaseColumn(qualifier = "totalSearchCost")
	public Long totalSearchCost;

	public static String generateRow(String requiredRowPrefix, String searchPage, Integer searchShowType, String typeValue){
		Util.checkEmptyString(typeValue);
		return generateRowPrefix(requiredRowPrefix, searchPage, searchShowType) + typeValue;
	}
	
	public static String generateRowPrefix(String requiredRowPrefix, String searchPage, Integer searchShowType){
		Util.checkNull(searchShowType);
		return generateRowPrefix(requiredRowPrefix, searchPage) + searchShowType  + RowUtil.ROW_SPLIT;
	}
	
	public static String generateRowPrefix(String requiredRowPrefix, String searchPage){
		Util.checkNull(searchPage);
		return requiredRowPrefix + searchPage + RowUtil.ROW_SPLIT;
	}
	
	public static String generateRequiredRowPrefix(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer resultType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		Util.checkNull(seId);
		Util.checkNull(resultType);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_SE_RESULT).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null?"": searchType).append(RowUtil.ROW_SPLIT);
		sb.append(resultType).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	public Long getSearchCount() {
		return searchCount;
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
}
