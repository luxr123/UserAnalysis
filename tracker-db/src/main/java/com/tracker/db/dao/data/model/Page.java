package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 文件名：Page
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:10:17
 * 功能描述：网站页面数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class Page {
	/**
	 * 默认常量值
	 */
	public static final String HOME_PAGE_SIGN = "index.php"; //首页标识
	public static final String OTHER_PAGE_SIGN = "other"; //其他页面标识
	
	/**
	 * row中各个字段index值
	 */
	public static final int WEB_ID_INDEX = 1;
	public static final int PAGE_SIGN_INDEX = 2;
	
	@HBaseColumn(qualifier = "pageTitle")
	public String pageTitle; //页面title
	
	@HBaseColumn(qualifier = "pageDesc")
	public String pageDesc; //页面描述
	
	/**
	 * 函数名：generateRowKey
	 * 功能描述：生成rowkey值
	 * @param webId 网站id
	 * @param pageSign 页面标识符
	 * @return
	 */
	public static String generateRowKey(Integer webId, String pageSign){
		Util.checkNull(pageSign);
		return generateRowPrefix(webId) + pageSign;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param webId
	 * @return
	 */
	public static String generateRowPrefix(Integer webId){
		Util.checkZeroValue(webId);
		return DataKeySign.SIGN_PAGE + RowUtil.ROW_SPLIT + webId + RowUtil.ROW_SPLIT;
	}
	
	/**
	 * 函数名：getPageSign
	 * 功能描述：获取url标记
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

	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}
}
