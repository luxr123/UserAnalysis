package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：WebSite
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:58:56
 * 功能描述：网站数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class WebSite {
	/**
	 * row中各个字段index值
	 */
	public static final int WEB_ID_INDEX = 1;
	
	@HBaseColumn(qualifier = "id")
	public Integer id; //网站id
	
	@HBaseColumn(qualifier = "domain")
	public String domain; //网站域名
	
	@HBaseColumn(qualifier = "desc")
	public String desc; //网站描述
	
	@HBaseColumn(qualifier = "urlPrefix")
	public String urlPrefix; //网站url前缀
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param webId 网站id
	 * @return
	 */
	public static String generateRow(int webId){
		return generateRowPrefix() + String.valueOf(webId);
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @return
	 */
	public static String generateRowPrefix(){
		return  DataKeySign.SIGN_WEBSITE + RowUtil.ROW_SPLIT;
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getUrlPrefix() {
		return urlPrefix;
	}

	public void setUrlPrefix(String urlPrefix) {
		this.urlPrefix = urlPrefix;
	}

	@Override
	public String toString(){
		return JsonUtil.toJson(this);
	}
}
