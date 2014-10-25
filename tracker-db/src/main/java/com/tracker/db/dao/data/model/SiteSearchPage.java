package com.tracker.db.dao.data.model;

import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchPage {
	public static final int WEB_ID_INDEX = 1;
	public static final int SEARCH_PAGE_ID_INDEX = 2;

	@HBaseColumn(qualifier = "searchPage")
	public String searchPage;
	
	@HBaseColumn(qualifier = "desc")
	public String desc;
	
	/**
	 * 生成搜索引擎row
	 */
	public static String generateRow(Integer webId, Integer searchPageId){
		Util.checkZeroValue(searchPageId);
		return generateRowPrefix(webId) + searchPageId;
	}
	
	public static String generateRowPrefix(Integer webId){
		Util.checkNull(webId);
		return generateRowPrefix() + webId + RowUtil.ROW_SPLIT;
	}
	
	public static String generateRowPrefix(){
		return DataKeySign.SIGN_SEARCH_PAGEE + RowUtil.ROW_SPLIT ;
	}

	public String getSearchPage() {
		return searchPage;
	}

	public void setSearchPage(String searchPage) {
		this.searchPage = searchPage;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
}
