package com.tracker.db.dao.kpi.model;

import java.io.Serializable;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 
 * 文件名：PageSummableKpi
 * 创建人：jason.hua
 * 创建日期：2014-10-27 下午12:38:35
 * 功能描述：基于页面的可累加kpi
 *
 */
@HBaseTable(tableName = "rt_kpi_summable", defaultFamily = "data")
public class PageSummableKpi implements Serializable{
	private static final long serialVersionUID = 1L;

	/**
	 * 文件名：Columns
	 * 创建人：jason.hua
	 * 创建日期：2014-10-27 下午12:41:18
	 * 功能描述：存储在hbase中的各个列名
	 *
	 */
	public static enum Columns{
		pv, entryPageCount, nextPageCount, outPageCount, stayTime
	}
	
	/**
	 * 标识值
	 */
	public static final String SIGN = "Page";
	
	/**
	 * row中各个字段的index值
	 */
	public static final int SIGN_INDEX = 0;
	public static final int DATE_INDEX = 1;
	public static final int WEB_ID_INDEX = 2;
	public static final int VISITOR_TYPE_INDEX = 3;
	public static final int PAGE_SIGN_INDEX = 4;

	@HBaseColumn(qualifier = "pv", isIncrementType = true)
	private Long pv; //浏览量
	
	@HBaseColumn(qualifier = "entryPageCount", isIncrementType = true)
	private Long entryPageCount; //入口页次数
	
	@HBaseColumn(qualifier = "nextPageCount", isIncrementType = true)
	private Long nextPageCount; //贡献下游页面数

	@HBaseColumn(qualifier = "outPageCount", isIncrementType = true)
	private Long outPageCount; // 退出页次数
	
	@HBaseColumn(qualifier = "stayTime", isIncrementType = true)
	private Long stayTime; //总停留时间
	

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
