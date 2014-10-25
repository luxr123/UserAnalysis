package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 搜索引擎，搜索条件，搜索类型
 * @author jason.hua
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchEngine {
	public static final int SE_ID_INDEX = 1;
	
	@HBaseColumn(qualifier = "seId")
	public Integer seId;
	
	@HBaseColumn(qualifier = "name")
	public String name;
	
	@HBaseColumn(qualifier = "desc")
	public String desc;
	
	/**
	 * 生成搜索引擎row
	 */
	public static String generateRow(Integer seId){
		Util.checkZeroValue(seId);
		return generateRowPrefix() + seId;
	}
	
	public static String generateRowPrefix(){
		return DataKeySign.SIGN_SEARCH_ENGINE + RowUtil.ROW_SPLIT;
	}
	
	public Integer getSeId() {
		return seId;
	}

	public void setSeId(Integer seId) {
		this.seId = seId;
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
