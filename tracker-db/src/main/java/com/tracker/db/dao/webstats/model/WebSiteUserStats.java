package com.tracker.db.dao.webstats.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_website_kpi", defaultFamily = "stats")
public class WebSiteUserStats {
	public final static String SIGN_WEB_USER = "web-user";  //基于用户类型

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int USER_TYPE_INDEX = 4;
	
	//kpi
	@HBaseColumn(qualifier = "uv")
	public Long uv;
	@HBaseColumn(qualifier = "ip_count")
	public Long ipCount;
	@HBaseColumn(qualifier = "pv")
	public Long pv;
	
	public static String generateRow(Integer webId, Integer timeType, String time, Integer userType){
		Util.checkNull(userType);
		return generateRowPrefix(webId, timeType, time) + userType;
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkNull(time);
		//webId + 日期类型 + 日期 + 用户类型
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_WEB_USER).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	public Long getPv() {
		return pv;
	}
	public void setPv(Long pv) {
		this.pv = pv;
	}
	public Long getUv() {
		return uv;
	}
	public void setUv(Long uv) {
		this.uv = uv;
	}
	public Long getIpCount() {
		return ipCount;
	}
	public void setIpCount(Long ipCount) {
		this.ipCount = ipCount;
	}

	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}
	
}
