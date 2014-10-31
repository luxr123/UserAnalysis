package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：UserTypeData
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:57:45
 * 功能描述：用户角色类型数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class UserTypeData {
	@HBaseColumn(qualifier = "userType")
	public Integer userType; //用户类型

	@HBaseColumn(qualifier = "desc")
	public String desc; //用户角色描述（中文名）
	
	@HBaseColumn(qualifier = "en_name")
	public String en_name; //用户角色英文名

	@HBaseColumn(qualifier = "isLogin")
	public Integer isLogin; //是否是登录用户
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成row
	 * @param webId 网站id
	 * @param userType 用户角色类型
	 * @return
	 */
	public static String generateRow(int webId, int userType){
		return generateRowPrefix(webId) + userType;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param webId 网站id
	 * @return
	 */
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