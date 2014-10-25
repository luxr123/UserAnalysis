package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 搜索条件
 * @author jason.hua
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchCondition {
	
	public static final int SE_ID_INDEX = 1;
	public static final int SEARCH_TYPE_INDEX = 2;
	public static final int SEARCH_CONDITION_INDEX = 3;
	
	@HBaseColumn(qualifier = "seConType")
	public Integer seConType;
	
	@HBaseColumn(qualifier = "name")
	public String name;
	
	@HBaseColumn(qualifier = "field")
	public String field;
	
	@HBaseColumn(qualifier = "isKeyword")
	public Integer isKeyword;
	
	@HBaseColumn(qualifier = "sortedNum")
	public Integer sortedNum;
	
	/**
	 * 生成搜索条件row
	 */
	public static String generateRow(Integer seId, Integer searchType, Integer seConType){
		return generateRowPrefix(seId, searchType) + seConType;
	}
	
	public static String generateRowPrefix(Integer seId, Integer searchType){
		return DataKeySign.SIGN_SEARCH_CONDITION  + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT + (searchType == null?"":searchType) + RowUtil.ROW_SPLIT;
	}
	
	public Integer getSeConType() {
		return seConType;
	}

	public void setSeConType(Integer seConType) {
		this.seConType = seConType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Integer getIsKeyword() {
		return isKeyword;
	}

	public void setIsKeyword(Integer isKeyword) {
		this.isKeyword = isKeyword;
	}

	public Integer getSortedNum() {
		return sortedNum;
	}

	public void setSortedNum(Integer sortedNum) {
		this.sortedNum = sortedNum;
	}

	@Override
	public String toString(){
		return JsonUtil.toJson(this);
	}
}
