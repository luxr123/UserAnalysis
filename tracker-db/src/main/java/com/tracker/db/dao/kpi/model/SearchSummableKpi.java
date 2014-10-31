package com.tracker.db.dao.kpi.model;

import java.io.Serializable;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：SearchSummableKpi
 * 创建人：jason.hua
 * 创建日期：2014-10-27 下午12:43:29
 * 功能描述：基于搜索页面的可累加指标
 *
 */
@HBaseTable(tableName = "rt_kpi_summable", defaultFamily = "data")
public class SearchSummableKpi implements Serializable{
	private static final long serialVersionUID = -105327334846624453L;
	/**
	 * 标识值
	 */
	public static final String SIGN_SE_RESULT = "se-result"; //基于结果类型
	public static final String SIGN_SE_PAGE_NUM = "se-page-num"; //基于结果类型
	public static final String SIGN_SE_CONDITION = "se-condition"; //基于搜索条件
	
	/**
	 * row中标识index值
	 */
	public static final int SIGN_INDEX = 0;

	/**
	 * 文件名：Columns
	 * 创建人：jason.hua
	 * 创建日期：2014-10-27 下午12:42:56
	 * 功能描述：存储在hbase中的各个列名
	 */
	public static enum Columns{
		pv, totalCost
	}

	@HBaseColumn(qualifier = "pv", isIncrementType = true)
	private Long pv; //搜索次数
	
	@HBaseColumn(qualifier = "totalCost", isIncrementType = true)
	private Long totalCost; //总搜索耗时
	
	/**
	 * 文件名：ResultRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-17 上午10:09:32
	 * 功能描述：基于搜索结果的row生成工具类
	 */
	public static class ResultRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int SEARCH_ID_INDEX = 3;
		public static final int SEARCH_TYPE_INDEX = 4;
		public static final int COST_TYPE_INDEX = 5;
		public static final int COUNT_TYPE_INDEX = 6;
		public static final int TIME_TYPE_INDEX = 7;
		public static final int SEARCH_PAGE_SHOW_INDEX = 8;

		public static String generateRowKey(String date, String webId, Integer seId, Integer searchType, Integer costType, Integer countType
				, Integer timeType, String searchPageShowType){
			StringBuffer sb = new StringBuffer();
			sb.append(generateRowPrefix(date, webId, seId, searchType));
			sb.append(costType).append(RowUtil.ROW_SPLIT);
			sb.append(countType).append(RowUtil.ROW_SPLIT);
			sb.append(timeType).append(RowUtil.ROW_SPLIT);
			sb.append(searchPageShowType);
			return sb.toString();
		}
		
		public static String generateRowPrefix(String date, String webId, Integer seId, Integer searchType){
			StringBuffer sb = new StringBuffer();
			sb.append(SIGN_SE_RESULT).append(RowUtil.ROW_SPLIT);
			sb.append(date).append(RowUtil.ROW_SPLIT);
			sb.append(webId).append(RowUtil.ROW_SPLIT);
			sb.append(seId).append(RowUtil.ROW_SPLIT);
			sb.append(searchType == null? "": searchType.toString()).append(RowUtil.ROW_SPLIT);
			return sb.toString();
		}
	}
	
	public static class PageNumResultGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int SEARCH_ID_INDEX = 3;
		public static final int SEARCH_TYPE_INDEX = 4;
		public static final int RESULT_TYPE_INDEX = 5;
		public static final int FIELD_VALUE_INDEX = 6;

		public static String generateRowKey(String date, String webId, Integer seId, Integer searchType, Integer resultType, Integer typeValue){
			return generateRowPrefix(date, webId, seId, searchType, resultType) + typeValue;
		}
		
		public static String generateRowPrefix(String date, String webId, Integer seId, Integer searchType, Integer resultType){
			StringBuffer sb = new StringBuffer();
			sb.append(SIGN_SE_PAGE_NUM).append(RowUtil.ROW_SPLIT);
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
	 * 功能描述：基于搜索条件的row生成工具类
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
}
