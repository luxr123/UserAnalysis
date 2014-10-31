package com.tracker.db.dao.siteSearch.entity;
/**
 * 
 * 文件名：SearchValueParam
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:18:07
 * 功能描述：废弃
 *
 */
public class SearchValueParam {
	private Integer searchConType;
	private String searchValue;
	private long searchCount;
	
	public SearchValueParam(Integer searchConType, String searchValue, long searchCount){
		this.searchConType = searchConType;
		this.searchValue = searchValue;
		this.searchCount = searchCount;
	}
	
	public Integer getSearchConType() {
		return searchConType;
	}

	public void setSearchConType(Integer searchConType) {
		this.searchConType = searchConType;
	}

	public String getSearchValue() {
		return searchValue;
	}
	public void setSearchValue(String searchValue) {
		this.searchValue = searchValue;
	}
	public long getSearchCount() {
		return searchCount;
	}
	public void setSearchCount(long searchCount) {
		this.searchCount = searchCount;
	}
	
	
}
