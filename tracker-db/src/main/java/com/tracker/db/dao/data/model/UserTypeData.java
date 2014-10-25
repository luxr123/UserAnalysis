package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class UserTypeData {
	@HBaseColumn(qualifier = "userType")
	public Integer userType;

	@HBaseColumn(qualifier = "desc")
	public String desc;
	
	@HBaseColumn(qualifier = "en_name")
	public String en_name;

	@HBaseColumn(qualifier = "isLogin")
	public Integer isLogin;
	
	public static String generateRow(int webId, int userType){
		return generateRowPrefix(webId) + userType;
	}
	
	public static String generateRowPrefix(int webId){
		return DataKeySign.SIGN_USER_TYPE + RowUtil.ROW_SPLIT + webId + RowUtil.ROW_SPLIT;
	}
	
	public Integer getUserType() {
		return userType;
	}

	public void setUserType(Integer userType) {
		this.userType = userType;
	}

	public String getEn_name() {
		return en_name;
	}

	public void setEn_name(String en_name) {
		this.en_name = en_name;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public Integer getIsLogin() {
		return isLogin;
	}

	public void setIsLogin(Integer isLogin) {
		this.isLogin = isLogin;
	}
	
	public String toString(){
		return JsonUtil.toJson(this);
	}
}