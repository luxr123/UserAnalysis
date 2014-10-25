package com.tracker.db.dao.webstats.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_website_kpi", defaultFamily = "stats")
public class WebSitePageStats {
	public final static String SIGN_WEB_PAGE = "web-page"; //基于访问页

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int VISITOR_TYPE_INDEX = 4;
	public static final int PAGE_SIGN_INDEX = 5;
	
	@HBaseColumn(qualifier = "pv")
	public Long pv;
	
	@HBaseColumn(qualifier = "uv")
	public Long uv;
	
	@HBaseColumn(qualifier = "ip_count")
	public Long ipCount;
	
	@HBaseColumn(qualifier = "entry_page_count")
	public Long entryPageCount;
	
	@HBaseColumn(qualifier = "next_page_count")
	public Long nextPageCount;
	
	@HBaseColumn(qualifier = "total_stay_time")
	public Long totalStayTime;
	
	@HBaseColumn(qualifier = "out_page_count")
	public Long outPageCount;
	
	public static String generateRow(Integer webId, Integer timeType, String time, Integer visitorType, String pageSign){
		Util.checkNull(pageSign);
		return generateRowPrefix(webId, timeType, time, visitorType) + pageSign;
	}
	
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time, Integer visitorType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_WEB_PAGE).append(RowUtil.ROW_SPLIT);
		sb.append(visitorType == null ? "":visitorType).append(RowUtil.ROW_SPLIT);
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
	public Long getTotalStayTime() {
		return totalStayTime;
	}
	public void setTotalStayTime(Long totalStayTime) {
		this.totalStayTime = totalStayTime;
	}
	public Long getOutPageCount() {
		return outPageCount;
	}
	public void setOutPageCount(Long outPageCount) {
		this.outPageCount = outPageCount;
	}
	
	
}
