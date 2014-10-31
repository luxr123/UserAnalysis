package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：SiteSearchCondition
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:44:47
 * 功能描述： 搜索条件数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchCondition {
	/**
	 * row中各个字段index值
	 */
	public static final int SE_ID_INDEX = 1;
	public static final int SEARCH_TYPE_INDEX = 2;
	public static final int SEARCH_CONDITION_INDEX = 3;
	
	@HBaseColumn(qualifier = "seConType")
	public Integer seConType; //搜索条件类型
	
	@HBaseColumn(qualifier = "name")
	public String name; //搜索条件中文名
	
	@HBaseColumn(qualifier = "field")
	public String field; //搜索条件字段（日志中的字段名）
	
	@HBaseColumn(qualifier = "isKeyword")
	public Integer isKeyword; //是否是关键词搜索
	
	@HBaseColumn(qualifier = "sortedNum")
	public Integer sortedNum; //页面上展示顺序编号
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成搜索条件row
	 * @param seId
	 * @param searchType
	 * @param seConType
	 * @return
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
