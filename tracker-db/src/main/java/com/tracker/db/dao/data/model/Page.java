package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class Page {
	public static final String HOME_PAGE_SIGN = "index.php";
	public static final String OTHER_PAGE_SIGN = "other";
	
	public static final int WEB_ID_INDEX = 1;
	public static final int PAGE_SIGN_INDEX = 2;
	
	@HBaseColumn(qualifier = "pageTitle")
	public String pageTitle;
	
	@HBaseColumn(qualifier = "pageDesc")
	public String pageDesc;
	
	/**
	 * 生成rowkey值
	 */
	public static String generateRowKey(Integer webId, String pageSign){
		Util.checkNull(pageSign);
		return generateRowPrefix(webId) + pageSign;
	}
	
	public static String generateRowPrefix(Integer webId){
		Util.checkZeroValue(webId);
		return DataKeySign.SIGN_PAGE + RowUtil.ROW_SPLIT + webId + RowUtil.ROW_SPLIT;
	}

	public String getPageTitle() {
		return pageTitle;
	}

	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	public String getPageDesc() {
		return pageDesc;
	}

	public void setPageDesc(String pageDesc) {
		this.pageDesc = pageDesc;
	}

	/**
	 * 获取url标记
	 * @param url
	 * @return
	 */
	public static String getPageSign(String url){
		if(url == null)
			return Page.HOME_PAGE_SIGN;
		String mainUrl = StringUtil.getMainUrl(url);
		String pageSign = StringUtil.removeDomain(mainUrl);
		//如果url为空，则返回主页页面
		if(pageSign.length() == 0){
			pageSign = Page.HOME_PAGE_SIGN;
		}
		return pageSign;
	}
	
	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}
}
