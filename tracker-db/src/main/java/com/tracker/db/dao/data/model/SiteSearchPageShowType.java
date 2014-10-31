package com.tracker.db.dao.data.model;

import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 文件名：SiteSearchPageShowType
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:55:24
 * 功能描述：站内搜索页面展示类型数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchPageShowType {
	/**
	 * row中各个字段index值
	 */
	public static final int WEB_ID_INDEX = 1;
	public static final int SE_ID_INDEX = 2;
	public static final int SEARCH_TYPE_INDEX = 3;
	public static final int SEARCH_PAGE_INDEX = 4;
	public static final int SHOW_TYPE_INDEX = 5;

	
	@HBaseColumn(qualifier = "name")
	public String name; //类型名
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param webId	网站id
	 * @param seId	站内搜索引擎id
	 * @param searchType 站内搜索引擎类型
	 * @param searchPage 站内搜索引擎页面
	 * @param showType 站内搜索页面页面展示类型
	 * @return
	 */
	public static String generateRow(Integer webId, Integer seId, Integer searchType, String searchPage, Integer showType){
		Util.checkZeroValue(showType);
		return generateRowPrefix(webId, seId, searchType, searchPage) + showType;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param webId	网站id
	 * @param seId	站内搜索引擎id
	 * @param searchType 站内搜索引擎类型
	 * @param searchPage 站内搜索引擎页面
	 * @return
	 */
	public static String generateRowPrefix(Integer webId, Integer seId, Integer searchType, String searchPage){
		Util.checkNull(searchPage);
		return generateRowPrefix(webId, seId, searchType) + searchPage + RowUtil.ROW_SPLIT;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param webId	网站id
	 * @param seId	站内搜索引擎id
	 * @param searchType 站内搜索引擎类型
	 * @return
	 */
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
