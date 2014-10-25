package com.tracker.db.dao.webstats.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 城市
 * @author jason
 *
 */
@HBaseTable(tableName = "offline_website_kpi", defaultFamily = "stats")
public class WebSiteCityStats {
	public final static String SIGN_WEB_CITY = "web-city";  //基于市

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int VISITOR_TYPE_INDEX = 4;
	public static final int COUNTRY_ID_INDEX = 5;
	public static final int PROVINCE_ID_INDEX = 6;
	public static final int CITY_ID_INDEX = 7;
	
	//kpi
	@HBaseColumn(qualifier = "uv")
	public Long uv;
	@HBaseColumn(qualifier = "ip_count")
	public Long ipCount;
	@HBaseColumn(qualifier = "pv")
	public Long pv;
	@HBaseColumn(qualifier = "visit_times")
	public Long visitTimes;//访问次数
	@HBaseColumn(qualifier = "total_visit_time")
	public Long totalVisitTime;// 总访问时长
	@HBaseColumn(qualifier = "jump_count")
	public Long jumpCount;//跳出次数
	
	public static String generateRow(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId, Integer provinceId, Integer cityId){
		Util.checkNull(cityId);
		return generateRowPrefix(webId, timeType, time, visitorType, countryId, provinceId) + cityId;
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId, Integer provinceId){
		Util.checkNull(webId);
		Util.checkNull(webId);
		Util.checkNull(time);
		Util.checkNull(countryId);
		Util.checkNull(provinceId);
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_WEB_CITY).append(RowUtil.ROW_SPLIT);
		sb.append(visitorType == null ? "":visitorType).append(RowUtil.ROW_SPLIT);
		sb.append(countryId).append(RowUtil.ROW_SPLIT);
		sb.append(provinceId).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	public Long getPv() {
		return pv;
	}
	public void setPv(Long pv) {
		this.pv = pv;
	}
	public Long getVisitTimes() {
		return visitTimes;
	}
	public void setVisitTimes(Long visitTimes) {
		this.visitTimes = visitTimes;
	}
	public Long getTotalVisitTime() {
		return totalVisitTime;
	}
	public void setTotalVisitTime(Long totalVisitTime) {
		this.totalVisitTime = totalVisitTime;
	}
	public Long getJumpCount() {
		return jumpCount;
	}
	public void setJumpCount(Long jumpCount) {
		this.jumpCount = jumpCount;
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
