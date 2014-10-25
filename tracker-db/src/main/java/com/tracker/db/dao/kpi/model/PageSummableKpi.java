package com.tracker.db.dao.kpi.model;

import java.io.Serializable;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

@HBaseTable(tableName = "rt_kpi_summable", defaultFamily = "data")
public class PageSummableKpi implements Serializable{
	private static final long serialVersionUID = 1L;

	public static enum Columns{
		pv, entryPageCount, nextPageCount, outPageCount, stayTime
	}
	public static final String SIGN = "Page";
	
	public static final int SIGN_INDEX = 0;
	public static final int DATE_INDEX = 1;
	public static final int WEB_ID_INDEX = 2;
	public static final int VISITOR_TYPE_INDEX = 3;
	public static final int PAGE_SIGN_INDEX = 4;

	//公用指标
	@HBaseColumn(qualifier = "pv", isIncrementType = true)
	private Long pv;
	
	//受访页指标
	@HBaseColumn(qualifier = "entryPageCount", isIncrementType = true)
	private Long entryPageCount;
	
	@HBaseColumn(qualifier = "nextPageCount", isIncrementType = true)
	private Long nextPageCount;

	@HBaseColumn(qualifier = "outPageCount", isIncrementType = true)
	private Long outPageCount;
	
	@HBaseColumn(qualifier = "stayTime", isIncrementType = true)
	private Long stayTime;
	

	public static String generateRowKey(String date, String webId, Integer visitorType, String pageSign){
		return generateRowPrefix(date, webId, visitorType) + pageSign;
	}
	
	public static String generateRowPrefix(String date, String webId,Integer visitorType){
		return generateRowPrefix(date, webId) + visitorType + RowUtil.ROW_SPLIT;
	}
	
	public static String generateRowPrefix(String date, String webId){
		StringBuffer sb = new StringBuffer();
		sb.append(SIGN).append(RowUtil.ROW_SPLIT);
		sb.append(date).append(RowUtil.ROW_SPLIT);
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	public Long getPv() {
		return pv;
	}

	public void setPv(Long pv) {
		this.pv = pv;
	}

	public Long getEntryPageCount() {
		return entryPageCount;
	}

	public void setEntryPageCount(Long entryPageCount) {
		this.entryPageCount = entryPageCount;
	}

	public Long getNextPageCount() {
		return nextPageCount;
	}

	public void setNextPageCount(Long nextPageCount) {
		this.nextPageCount = nextPageCount;
	}

	public Long getOutPageCount() {
		return outPageCount;
	}

	public void setOutPageCount(Long outPageCount) {
		this.outPageCount = outPageCount;
	}

	public Long getStayTime() {
		return stayTime;
	}

	public void setStayTime(Long stayTime) {
		this.stayTime = stayTime;
	}
}
