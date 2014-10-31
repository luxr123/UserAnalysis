package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 文件名：VisitTypeData
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:58:32
 * 功能描述：访问类型数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class VisitTypeData {
	public final static String VISIT_TYPE = "visit";//默认浏览访问类型

	@HBaseColumn(qualifier = "visitType")
	public Integer visitType; //访问类型

	@HBaseColumn(qualifier = "desc")
	public String desc; //访问类型描述
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param webId 网站id
	 * @param sign 类型标识（visit, FoxEngine, CaseEngine等）
	 * @param searchType 搜索引擎类型（对于浏览类型则为null）
	 * @return
	 */
	public static String generateRow(Integer webId, String sign, Integer searchType){
		Util.checkNull(webId);
		Util.checkNull(sign);
		return generateRowPrefix(webId) + sign + RowUtil.ROW_SPLIT + (searchType == null?"" : searchType);
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述： 生成row前缀
	 * @param webId 网站id
	 * @return
	 */
	public static String generateRowPrefix(int webId){
		return DataKeySign.SIGN_VISIT_TYPE + RowUtil.ROW_SPLIT + webId + RowUtil.ROW_SPLIT;
	}
	
	public Integer getVisitType() {
		return visitType;
	}

	public void setVisitType(Integer visitType) {
		this.visitType = visitType;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String toString(){
		return JsonUtil.toJson(this);
	}
}