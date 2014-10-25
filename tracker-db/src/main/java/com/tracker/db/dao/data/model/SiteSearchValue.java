package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchValue {
	public static final int SITE_SEARCH_INDEX = 1;
	public static final int SITE_SEARCH_CON_INDEX = 2;
	public static final int SITE_SEARCH_VALUE_INDEX = 3;
	
	@HBaseColumn(qualifier = "name_ch")
	public String name_ch;
	
	public String getName_ch() {
		return name_ch;
	}

	public void setName_ch(String name_ch) {
		this.name_ch = name_ch;
	}

	public static String generateRow(int seId, int conType, String idStr){
		return generateRowPrefix(seId, conType) + idStr;
	}
	
	public static String generateRowPrefix(int seId, int conType){
		return DataKeySign.SIGN_SEARCH_VALUE + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT + conType + RowUtil.ROW_SPLIT;
	}
	
	public String toString(){
		return JsonUtil.toJson(this);
	}
}
	