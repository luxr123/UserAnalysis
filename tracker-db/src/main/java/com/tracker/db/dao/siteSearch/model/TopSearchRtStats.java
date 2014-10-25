package com.tracker.db.dao.siteSearch.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_search_top_res_time", defaultFamily = "stats")
public class TopSearchRtStats {
	public static final String SIGN_SE_TOP_RT = "se-top-rt"; //最慢响应时间

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int SEARCH_ENGINE_INDEX = 4;
	public static final int SEARCH_TYPE_INDEX = 5;
	public static final int RESPONSE_TIME = 6;
	public static final int NUM = 7;
	
	@HBaseColumn(qualifier = "userType")
	public Integer userType;
	
	@HBaseColumn(qualifier = "userId")
	public Integer userId;
	
	@HBaseColumn(qualifier = "cookieId")
	public String cookieId;
	
	@HBaseColumn(qualifier = "ip")
	public String ip;
	
	@HBaseColumn(qualifier = "searchType")
	public Integer searchType;
	
	@HBaseColumn(qualifier = "serverTime")
	public Long serverTime;
	
	@HBaseColumn(qualifier = "responseTime")
	public Integer responseTime;
	
	@HBaseColumn(qualifier = "totalResultCount")
	public Long totalResultCount;
	
	@HBaseColumn(qualifier = "searchValueStr")
	public String searchValueStr;
	
	public static String generateRow(Integer webId, Integer timeType, String time, Integer searchEngineId, Integer responseTime, Integer searchType, Integer num){
		Util.checkNull(num);
		Util.checkNull(responseTime);
		return generateRowPrefix(webId, timeType, time, searchEngineId, searchType) + (Integer.MAX_VALUE - responseTime) + RowUtil.ROW_SPLIT + num;
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time, Integer searchEngineId, Integer searchType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		Util.checkNull(searchEngineId);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_SE_TOP_RT).append(RowUtil.ROW_SPLIT);
		sb.append(searchEngineId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null? "":searchType).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	public Long getServerTime() {
		return serverTime;
	}

	public void setServerTime(Long serverTime) {
		this.serverTime = serverTime;
	}

	public Integer getSearchType() {
		return searchType;
	}

	public void setSearchType(Integer searchType) {
		this.searchType = searchType;
	}

	public Integer getResponseTime() {
		return responseTime;
	}

	public void setResponseTime(Integer responseTime) {
		this.responseTime = responseTime;
	}

	public Long getTotalResultCount() {
		return totalResultCount;
	}

	public void setTotalResultCount(Long totalResultCount) {
		this.totalResultCount = totalResultCount;
	}

	public Integer getUserType() {
		return userType;
	}

	public void setUserType(Integer userType) {
		this.userType = userType;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public String getCookieId() {
		return cookieId;
	}

	public void setCookieId(String cookieId) {
		this.cookieId = cookieId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getSearchValueStr() {
		return searchValueStr;
	}

	public void setSearchValueStr(String searchValueStr) {
		this.searchValueStr = searchValueStr;
	}
}
