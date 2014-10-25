package com.tracker.db.dao.data.model;

import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchPageShowType {
	public static final int WEB_ID_INDEX = 1;
	public static final int SE_ID_INDEX = 2;
	public static final int SEARCH_TYPE_INDEX = 3;
	public static final int SEARCH_PAGE_INDEX = 4;
	public static final int SHOW_TYPE_INDEX = 5;

	
	@HBaseColumn(qualifier = "name")
	public String name;
	
	/**
	 * 生成搜索引擎row
	 */
	public static String generateRow(Integer webId, Integer seId, Integer searchType, String searchPage, Integer showType){
		Util.checkZeroValue(showType);
		return generateRowPrefix(webId, seId, searchType, searchPage) + showType;
	}
	
	public static String generateRowPrefix(Integer webId, Integer seId, Integer searchType, String searchPage){
		Util.checkNull(searchPage);
		return generateRowPrefix(webId, seId, searchType) + searchPage + RowUtil.ROW_SPLIT;
	}
	
	public static String generateRowPrefix(Integer webId, Integer seId, Integer searchType){
		Util.checkZeroValue(webId);
		Util.checkZeroValue(seId);
		return DataKeySign.SIGN_SEARCH_PAGE_SHOW_TYPE + RowUtil.ROW_SPLIT + webId + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT + (searchType == null?"":searchType) + RowUtil.ROW_SPLIT;
	}
	
	public static String generateAllRowPrefix(){
		return DataKeySign.SIGN_SEARCH_PAGE_SHOW_TYPE + RowUtil.ROW_SPLIT ;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
