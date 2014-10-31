package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 文件名：SiteSearchEngine
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:50:23
 * 功能描述：站内搜索引擎数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchEngine {
	/**
	 * row中各个字段index值
	 */
	public static final int SE_ID_INDEX = 1;
	
	@HBaseColumn(qualifier = "seId")
	public Integer seId; //搜索引擎id
	
	@HBaseColumn(qualifier = "name")
	public String name; //搜索引擎名
	
	@HBaseColumn(qualifier = "desc")
	public String desc; //搜索引擎描述
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成搜索引擎row
	 * @param seId 搜索引擎id
	 * @return
	 */
	public static String generateRow(Integer seId){
		Util.checkZeroValue(seId);
		return generateRowPrefix() + seId;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成搜索引擎row前缀
	 * @return
	 */
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
