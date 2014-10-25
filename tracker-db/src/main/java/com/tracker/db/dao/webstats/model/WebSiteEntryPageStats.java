package com.tracker.db.dao.webstats.model;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_website_kpi", defaultFamily = "stats")
public class WebSiteEntryPageStats {
	public final static String SIGN_WEB_ENTRY_PAGE = "web-entry-page";  //基于入口 页

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
	
	@HBaseColumn(qualifier = "total_visit_page")
	public Long totalVisitPage;
	
	@HBaseColumn(qualifier = "jump_count")
	public Long jumpCount;
	
	@HBaseColumn(qualifier = "total_visit_time")
	public Long totalVisitTime;
	
	public static String generateRow(Integer webId, Integer timeType, String time, Integer visitorType, String pageSign){
		Util.checkNull(pageSign);
		return generateRowPrefix(webId, timeType, time, visitorType) + pageSign;
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time, Integer visitorType){
		return generateRowPrefix(webId, timeType, time) + (visitorType == null ? "":visitorType) + RowUtil.ROW_SPLIT;
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_WEB_ENTRY_PAGE).append(RowUtil.ROW_SPLIT);
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
	
	public Long getTotalVisitPage() {
		return totalVisitPage;
	}

	public void setTotalVisitPage(Long totalVisitPage) {
		this.totalVisitPage = totalVisitPage;
	}

	public Long getJumpCount() {
		return jumpCount;
	}

	public void setJumpCount(Long jumpCount) {
		this.jumpCount = jumpCount;
	}

	public Long getTotalVisitTime() {
		return totalVisitTime;
	}

	public void setTotalVisitTime(Long totalVisitTime) {
		this.totalVisitTime = totalVisitTime;
	}
	
}
