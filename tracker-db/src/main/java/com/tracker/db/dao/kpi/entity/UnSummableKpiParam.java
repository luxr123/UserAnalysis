package com.tracker.db.dao.kpi.entity;

import com.tracker.db.util.RowUtil;

/**
 * 文件名：UnSummableKpiParam
 * 创建人：jason.hua
 * 创建日期：2014-10-27 下午12:09:29
 * 功能描述：不可累加kpi的row生成类
 *
 */
public class UnSummableKpiParam {
	/**
	 * 网站业务标识
	 */
	public final static String SIGN_WEB_USER = "web-user";  //基于用户类型
	public final static String SIGN_WEB_DATE = "web-date"; //基于日期
	public final static String SIGN_WEB_TIME = "web-time"; //基于小时段
	public final static String SIGN_WEB_REF = "web-ref"; //基于来源类型
	public final static String SIGN_WEB_REF_DOMAIN = "web-ref-domain"; //基于外部链接、外部搜索引擎
	public final static String SIGN_WEB_REF_KEYWORD = "web-ref-keyword"; //基于外部搜索引擎关键词
	public final static String SIGN_WEB_PROVINCE = "web-province"; //基于省份
	public final static String SIGN_WEB_CITY = "web-city";  //基于市
	public final static String SIGN_WEB_PAGE = "web-page"; //基于访问页
	public final static String SIGN_WEB_ENTRY_PAGE = "web-entry-page";  //基于入口 页
	public final static String SIGN_WEB_SYS = "web-sys"; //基于系统环境（前缀，需要加具体系统环境类型值）

	/**
	 * 站内搜索页面标识
	 */
	public static final String SIGN_SE_DATE = "se-date"; //基于日期
	
	/**
	 * kpi标识
	 */
	public static final String KPI_IP = "ip";
	public static final String KPI_UV = "uv";
	
	/**
	 * 
	 * 文件名：WebSiteRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-16 下午2:53:21
	 * 功能描述：网站业务row生成工具
	 *
	 */
	public static class WebSiteRowGenerator{
		/**
		 * row中各个字段的index值
		 */
		public static final int SIGN_INDEX = 0;
		public static final int KPI_INDEX = 1;
		public static final int TIME_INDEX = 2;
		public static final int WEB_ID_INDEX = 3;
		public static final int VISITOR_TYPE_INDEX = 4;
		public static final int FIELD_INDEX = 5;
		public static final int IP_OR_COOKIE = 6;

		/**
		 * 函数名：generateRowKey
		 * 功能描述：生成row
		 * @param sign 业务标识
		 * @param kpi kpi标识
		 * @param time 时间， 分为年（YYYY）、月（YYYYMM）、周（YYYYWW）、日（YYYYMMDD）
		 * @param webId 网站id
		 * @param visitorType 访问者类型（新老访客）
		 * @param field 字段值（如用户角色类型值、小时段值、来源域名值等）
		 * @param ipOrCookieId ip或者cookieId值
		 * @return
		 */
		public static String generateRowKey(String sign, String kpi, String time, String webId, Integer visitorType, String field, String ipOrCookieId){
			return generateRowPrefix(sign, kpi, time, webId, visitorType, field) + ipOrCookieId;
		}
		
		/**
		 * 函数名：generateRowPrefix
		 * 功能描述：生成基于网站统计的row前缀
		 * @param sign 业务标识
		 * @param kpi kpi标识
		 * @param time 时间， 分为年（YYYY）、月（YYYYMM）、周（YYYYWW）、日（YYYYMMDD）
		 * @param webId 网站id
		 * @param visitorType 访问者类型（新老访客）
		 * @param field 字段值（如用户角色类型值、小时段值、来源域名值等）
		 * @return
		 */
		public static String generateRowPrefix(String sign, String kpi, String time, String webId, Integer visitorType, String field){
			StringBuffer sb = new StringBuffer();
			sb.append(sign).append(RowUtil.ROW_SPLIT);
			sb.append(kpi).append(RowUtil.ROW_SPLIT);
			sb.append(time).append(RowUtil.ROW_SPLIT);
			sb.append(webId).append(RowUtil.ROW_SPLIT);
			sb.append(visitorType == null? "": visitorType).append(RowUtil.ROW_SPLIT);
			sb.append(field).append(RowUtil.ROW_SPLIT);
			return sb.toString();
		}
	}
	
	/**
	 * 文件名：SearchRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-16 下午2:53:25
	 * 功能描述：基于站内搜索业务的row工具类
	 */
	public static class SearchRowGenerator{
		/**
		 * row中各个字段的index值
		 */
		public static final int SIGN_INDEX = 0;
		public static final int KPI_INDEX = 1;
		public static final int TIME_INDEX = 2;
		public static final int WEB_ID_INDEX = 3;
		public static final int SEARCH_ID_INDEX = 4;
		public static final int SEARCH_TYPE_INDEX = 5;
		public static final int IP_OR_COOKIE = 6;

		/**
		 * 函数名：generateRowKey
		 * 功能描述：生成基于站内搜索业务的row
		 * @param sign 业务标识
		 * @param kpi kpi标识
		 * @param time 时间， 分为年（YYYY）、月（YYYYMM）、周（YYYYWW）、日（YYYYMMDD）
		 * @param webId 网站id
		 * @param seId 站内搜索引擎id
		 * @param searchType 站内搜索引擎类型
		 * @param ipOrCookieId ip或cookieId值
		 * @return
		 */
		public static String generateRowKey(String sign, String kpi, String time, String webId, Integer seId, Integer searchType, String ipOrCookieId){
			return generateRowPrefix(sign, kpi, time, webId, seId, searchType) + ipOrCookieId;
		}
		
		/**
		 * 函数名：generateRowPrefix
		 * 功能描述：生成基于站内搜索业务的row前缀
		 * @param sign 业务标识
		 * @param kpi kpi标识
		 * @param time 时间， 分为年（YYYY）、月（YYYYMM）、周（YYYYWW）、日（YYYYMMDD）
		 * @param webId 网站id
		 * @param seId 站内搜索引擎id
		 * @param searchType 站内搜索引擎类型
		 * @return
		 */
		public static String generateRowPrefix(String sign, String kpi, String time, String webId, Integer seId, Integer searchType){
			StringBuffer sb = new StringBuffer();
			sb.append(sign).append(RowUtil.ROW_SPLIT);
			sb.append(kpi).append(RowUtil.ROW_SPLIT);
			sb.append(time).append(RowUtil.ROW_SPLIT);
			sb.append(webId).append(RowUtil.ROW_SPLIT);
			sb.append(seId).append(RowUtil.ROW_SPLIT);
			sb.append(searchType == null? "": searchType).append(RowUtil.ROW_SPLIT);
			return sb.toString();
		}
	}
}
