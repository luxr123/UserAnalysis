package com.tracker.storm.common.indexbolt;

import java.util.List;

import com.tracker.common.log.UserVisitLogFields.FIELDS;
/**
 * 
 * 文件名：Cookie_UserFieldInex
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:52:43
 * 功能描述：为userId,cookieId,ip创建索引.
 *
 */
public class CookieUserFieldIndexBolt extends FieldIndexBolt {

	public CookieUserFieldIndexBolt(FIELDS indexField, String table,String zookeeper) {
		super(indexField, table,zookeeper);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 160687021277713059L;

	@Override
	public List<String> getInputFields() {
		// TODO Auto-generated method stub
		List<String> retVal = super.getInputFields();
		retVal.add(FIELDS.userType.toString());
		return retVal;
	}
	
	public static String getCompentId(){
		return "cookie_userBolt";
	}

}
