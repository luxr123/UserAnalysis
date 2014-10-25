package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * row = {ROW_PREFIX, 网站id}
 * @author jason.hua
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class WebSite {
	public static final int WEB_ID_INDEX = 1;
	
	@HBaseColumn(qualifier = "id")
	public Integer id;
	
	@HBaseColumn(qualifier = "domain")
	public String domain;
	
	@HBaseColumn(qualifier = "desc")
	public String desc;
	
	@HBaseColumn(qualifier = "urlPrefix")
	public String urlPrefix;
	
	/**
	 * 生成rowkey值
	 */
	public static String generateRow(int webId){
		return generateRowPrefix() + String.valueOf(webId);
	}
	
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
