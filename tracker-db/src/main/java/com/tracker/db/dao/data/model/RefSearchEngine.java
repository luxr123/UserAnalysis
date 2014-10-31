package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：RefSearchEngine
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:13:27
 * 功能描述：外部搜索引擎数据字段
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class RefSearchEngine {
	/**
	 * row中各个字段index值
	 */
	public static final int REF_TYPE_INDEX = 1;
	public static final int ID_INDEX = 2;
	
	@HBaseColumn(qualifier = "domain", isStoreStringType = true)
	public String domain; // 搜索引擎域名
	
	@HBaseColumn(qualifier = "name", isStoreStringType = true)
	public String name; //搜索引擎中文名
	
	public RefSearchEngine(){}
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成rowKey
	 * @param id id号
	 * @return
	 */
	public static String generateRow(int id){
		return generateRowPrefix() + id;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成
	 * @return
	 */
	public static String generateRowPrefix(){
		return DataKeySign.SIGN_REF_SE + RowUtil.ROW_SPLIT;
	}
	
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}
}
