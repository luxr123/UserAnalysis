package com.tracker.db.dao.siteSearch.entity;

import java.util.ArrayList;
import java.util.List;
/**
 * 
 * 文件名：SearchTopResult
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:17:20
 * 功能描述：用于drpc服务返回最多搜索值,废弃
 *
 */
@Deprecated
public class SearchTopResult {
	private List<Entry> list = new ArrayList<Entry>();
	private long totalCount;
	
	public List<Entry> getList() {
		return list;
	}

	public void setList(List<Entry> list) {
		this.list = list;
	}
	
	public void addEntry(String field, long searchCount){
		list.add(new Entry(field, searchCount));
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public static class Entry{
		private String field;
		private long searchCount;
		
		public Entry(String field, long searchCount){
			this.field = field;
			this.searchCount = searchCount;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field;
		}

		public long getSearchCount() {
			return searchCount;
		}

		public void setSearchCount(long searchCount) {
			this.searchCount = searchCount;
		}
		
	}
}
