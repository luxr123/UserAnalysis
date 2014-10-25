package com.tracker.db.dao.kpi.model;

import java.io.Serializable;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

@HBaseTable(tableName = "rt_kpi_summable", defaultFamily = "data")
public class SearchSummableKpi implements Serializable{
	private static final long serialVersionUID = -105327334846624453L;
	//sign for search
	public static final String SIGN_SE_RESULT = "se-result"; //基于结果类型
	public static final String SIGN_SE_CONDITION = "se-condition"; //基于搜索条件
	public static final int SIGN_INDEX = 0;

	public static enum Columns{
		pv, totalCost
	}

	//公用指标
	@HBaseColumn(qualifier = "pv", isIncrementType = true)
	private Long pv;
	
	//站内搜索指标
	@HBaseColumn(qualifier = "totalCost", isIncrementType = true)
	private Long totalCost;
	
	/**
	 * 
	 * 文件名：ResultRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-17 上午10:09:32
	 * 功能描述：
	 *
	 */
	public static class ResultRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int SEARCH_ID_INDEX = 3;
		public static final int SEARCH_TYPE_INDEX = 4;
		public static final int RESULT_TYPE_INDEX = 5;
		public static final int SEARCH_PAGE_SHOW_INDEX = 6;
		public static final int FIELD_VALUE_INDEX = 7;

		public static String generateRowKey(String date, String webId, Integer seId, Integer searchType, Integer resultType, String searchPageShowType, Integer typeValue){
			return generateRowPrefix(date, webId, seId, searchType, resultType) + searchPageShowType + RowUtil.ROW_SPLIT + typeValue;
		}
		
		public static String generateRowPrefix(String date, String webId, Integer seId, Integer searchType, Integer resultType){
			StringBuffer sb = new StringBuffer();
			sb.append(SIGN_SE_RESULT).append(RowUtil.ROW_SPLIT);
			sb.append(date).append(RowUtil.ROW_SPLIT);
			sb.append(webId).append(RowUtil.ROW_SPLIT);
			sb.append(seId).append(RowUtil.ROW_SPLIT);
			sb.append(searchType == null? "": searchType.toString()).append(RowUtil.ROW_SPLIT);
			sb.append(resultType).append(RowUtil.ROW_SPLIT);
			return sb.toString();
		}
	}
	
	/**
	 * 
	 * 文件名：ConditionRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-17 上午10:09:36
	 * 功能描述：
	 *
	 */
	public static class ConditionRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int SEARCH_ID_INDEX = 3;
		public static final int SEARCH_TYPE_INDEX = 4;
		public static final int CONDITION_TYPE_INDEX = 5;

		public static String generateRowKey(String date, String webId, Integer seId, Integer searchType, Integer seConType){
			return generateRowPrefix(date, webId, seId, searchType) + seConType;
		}
		
		public static String generateRowPrefix(String date, String webId, Integer seId, Integer searchType){
			StringBuffer sb = new StringBuffer();
			sb.append(SIGN_SE_CONDITION).append(RowUtil.ROW_SPLIT);
			sb.append(date).append(RowUtil.ROW_SPLIT);
			sb.append(webId).append(RowUtil.ROW_SPLIT);
			sb.append(seId).append(RowUtil.ROW_SPLIT);
			sb.append(searchType == null? "": searchType.toString()).append(RowUtil.ROW_SPLIT);
			return sb.toString();
		}
	}
	
	public Long getPv() {
		return pv;
	}

	public void setPv(Long pv) {
		this.pv = pv;
	}

	public Long getTotalCost() {
		return totalCost;
	}

	public void setTotalCost(Long totalCost) {
		this.totalCost = totalCost;
	}

	public static String generateRowKey(String date, String webId, String sign, Integer seId, Integer searchType, String field){
		return generateRowPrefix(date, webId, sign, seId, searchType) + field;
	}
	
	public static String generateRowKey(String requiredRowPrefix, String field){
		return requiredRowPrefix + field;
	}
	
	public static String generateRowPrefix(String date, String webId, String sign, Integer seId, Integer searchType){
		StringBuffer sb = new StringBuffer();
		sb.append(date).append(RowUtil.ROW_SPLIT);
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(sign).append(RowUtil.ROW_SPLIT);
		sb.append(seId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null? "": searchType.toString()).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	
}
