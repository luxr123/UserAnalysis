package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;


@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class RefSearchEngine {
	
	public static final int REF_TYPE_INDEX = 1;
	public static final int ID_INDEX = 2;
	
	@HBaseColumn(qualifier = "domain", isStoreStringType = true)
	public String domain;
	
	@HBaseColumn(qualifier = "name", isStoreStringType = true)
	public String name;
	
	public RefSearchEngine(){}
	
	public static String generateRow(int id){
		return generateRowPrefix() + id;
	}
	
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
