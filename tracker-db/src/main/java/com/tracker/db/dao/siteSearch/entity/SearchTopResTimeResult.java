package com.tracker.db.dao.siteSearch.entity;

import java.util.ArrayList;
import java.util.List;

import com.tracker.common.utils.JsonUtil;
/**
 * 
 * 文件名：SearchTopResTimeResult
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:16:41
 * 功能描述：废弃
 *
 */
public class SearchTopResTimeResult {
	private List<ResponseTimeRecord> list = new ArrayList<ResponseTimeRecord>();
	private long totalCount;
	
	public List<ResponseTimeRecord> getList() {
		return list;
	}

	public void setList(List<ResponseTimeRecord> list) {
		this.list = list;
	}
	
	public void addRecord(ResponseTimeRecord record) {
		list.add(record);
	}		

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public static class ResponseTimeRecord {
		private Integer userType;
		private String userId;
		private String cookieId;
		private String ip;
		private Integer responseTime;
		private Long searchTime;
		private Long totalCount;
		private String searchParam;
		
		public ResponseTimeRecord(){
			
		}
		
		public ResponseTimeRecord(Integer responseTime, Long searchTime, Long totalCount, String searchParam){
			this.responseTime = responseTime;
			this.searchTime = searchTime;
			this.totalCount = totalCount;
			this.searchParam = searchParam;
		}
		
		public Integer getUserType() {
			return userType;
		}

		public void setUserType(Integer userType) {
			this.userType = userType;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
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

		public Integer getResponseTime() {
			return responseTime;
		}

		public void setResponseTime(Integer responseTime) {
			this.responseTime = responseTime;
		}

		public Long getSearchTime() {
			return searchTime;
		}

		public void setSearchTime(Long searchTime) {
			this.searchTime = searchTime;
		}

		public Long getTotalCount() {
			return totalCount;
		}

		public void setTotalCount(Long totalCount) {
			this.totalCount = totalCount;
		}

		public String getSearchParam() {
			return searchParam;
		}

		public void setSearchParam(String searchParam) {
			this.searchParam = searchParam;
		}

		public String toString(){
			return JsonUtil.toJson(this);
		}
		
		public static ResponseTimeRecord toObj(String json){
			return JsonUtil.toObject(json, ResponseTimeRecord.class);
		}
	}

}
