package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：SiteSearchValue
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:57:10
 * 功能描述：站内搜索值数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchValue {
	/**
	 * row中各个字段index值
	 */
	public static final int SITE_SEARCH_INDEX = 1;
	public static final int SITE_SEARCH_CON_INDEX = 2;
	public static final int SITE_SEARCH_VALUE_INDEX = 3;
	
	@HBaseColumn(qualifier = "name_ch")
	public String name_ch; //中文名
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param seId 站内搜索引擎id
	 * @param conType 站内搜索条件类型
	 * @param idStr 搜索值标识
	 * @return
	 */
	public static String generateRow(int seId, int conType, String idStr){
		return generateRowPrefix(seId, conType) + idStr;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param seId 站内搜索引擎id
	 * @param conType 站内搜索条件类型
	 * @return
	 */
	public static String generateRowPrefix(int seId, int conType){
		return DataKeySign.SIGN_SEARCH_VALUE + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT + conType + RowUtil.ROW_SPLIT;
	}
	
	public String getName_ch() {
		return name_ch;
	}

	public void setName_ch(String name_ch) {
		this.name_ch = name_ch;
	}
	
	public String toString(){
		return JsonUtil.toJson(this);
	}
}
	