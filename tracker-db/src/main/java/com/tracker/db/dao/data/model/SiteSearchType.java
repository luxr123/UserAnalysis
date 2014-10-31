package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 
 * 文件名：SiteSearchType
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:56:42
 * 功能描述：站内搜索引擎类型数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchType {
	/**
	 * row中各个字段index值
	 */
	public static final int SE_ID_INDEX = 1;
	public static final int SE_TYPE_INDEX = 2;
	
	@HBaseColumn(qualifier = "name")
	public String name; //类型名
	
	@HBaseColumn(qualifier = "desc")
	public String desc; //类型描述
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param seId 站内搜索引擎id
	 * @param seType 站内搜索引擎类型
	 * @return
	 */
	public static String generateRow(Integer seId, Integer seType){
		return generateRowPrefix(seId) + seType;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param seId 站内搜索引擎id
	 * @return
	 */
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
