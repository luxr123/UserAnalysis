package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 站内搜索引擎类型
 * @author jason.hua
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchType {
	public static final int SE_ID_INDEX = 1;
	public static final int SE_TYPE_INDEX = 2;
	
	@HBaseColumn(qualifier = "name")
	public String name;
	
	@HBaseColumn(qualifier = "desc")
	public String desc;
	
	/**
	 * 生成搜索类型row
	 */
	public static String generateRow(Integer seId, Integer seType){
		return generateRowPrefix(seId) + seType;
	}
	
	public static String generateRowPrefix(Integer seId){
		return DataKeySign.SIGN_SEARCH_TYPE + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}


	@Override
	public String toString(){
		return JsonUtil.toJson(this);
	}
}
