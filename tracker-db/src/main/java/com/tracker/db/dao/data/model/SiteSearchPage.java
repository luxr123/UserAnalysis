package com.tracker.db.dao.data.model;

import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 文件名：SiteSearchPage
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:52:17
 * 功能描述：站内搜索页面数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchPage {
	/**
	 * row中各个字段index值
	 */
	public static final int WEB_ID_INDEX = 1;
	public static final int SEARCH_PAGE_ID_INDEX = 2;

	@HBaseColumn(qualifier = "searchPage")
	public String searchPage; //搜索页面
	
	@HBaseColumn(qualifier = "desc")
	public String desc; //搜索页面描述
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param webId	 网站id
	 * @param searchPageId 搜索引擎id
	 * @return
	 */
	public static String generateRow(Integer webId, Integer searchPageId){
		Util.checkZeroValue(searchPageId);
		return generateRowPrefix(webId) + searchPageId;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param webId 网站id
	 * @return
	 */
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
