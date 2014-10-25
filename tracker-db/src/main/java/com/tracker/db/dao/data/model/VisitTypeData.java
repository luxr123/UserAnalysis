package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class VisitTypeData {
	@HBaseColumn(qualifier = "visitType")
	public Integer visitType;

	@HBaseColumn(qualifier = "desc")
	public String desc;
	
	public static String generateRow(Integer webId, String sign, Integer searchType){
		Util.checkNull(webId);
		Util.checkNull(sign);
		return generateRowPrefix(webId) + sign + RowUtil.ROW_SPLIT + (searchType == null?"" : searchType);
	}
	
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