package com.tracker.db.dao.kpi.model;

import java.io.Serializable;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

@HBaseTable(tableName = "rt_kpi_summable", defaultFamily = "data")
public class WebSiteSummableKpi  implements Serializable{
	private static final long serialVersionUID = -8603052370936191904L;

	/**
	 * 文件名：Columns
	 * 创建人：jason.hua
	 * 创建日期：2014-10-27 下午12:41:18
	 * 功能描述：存储在hbase中的各个列名
	 *
	 */
	public static enum Columns{
		pv, visitTimes, totalVisitTime, totalJumpCount, totalVisitPage
	}
	
	/**
	 * 标识值
	 */
	public static final String SIGN_BASIC = "basic";
	public static final String SIGN_REF_KW = "ref-kw";
	public static final String SIGN_ENTRY_PAGE = "entryPage";
	public static final String SIGN_AREA = "area";
	public static final String SIGN_SYS_BASIC = "sys-env"; //浏览器、操作系统、屏幕颜色、是否支持cookie、语言环境、分辨率

	/**
	 * row中标识index值
	 */
	public static final int SIGN_INDEX = 0;

	@HBaseColumn(qualifier = "pv", isIncrementType = true)
	private Long pv; //浏览量
	
	//会话指标
	@HBaseColumn(qualifier = "visitTimes", isIncrementType = true)
	private Long visitTimes; //访问次数
	
	@HBaseColumn(qualifier = "totalVisitTime", isIncrementType = true)
	private Long totalVisitTime; //总访问时间
	
	@HBaseColumn(qualifier = "totalJumpCount", isIncrementType = true)
	private Long totalJumpCount; //总跳出次数
	
	@HBaseColumn(qualifier = "totalVisitPage", isIncrementType = true)
	private Long totalVisitPage; //总访问页数
	
	/**
	 * 文件名：BasicRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-15 下午4:53:10
	 * 功能描述：基于日期、小时段、用户角色、来源类型、来源域名的row生成工具类
	 */
	public static class BasicRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int VISITOR_TYPE_INDEX = 3;
		public static final int TIME_INDEX = 4;
		public static final int USER_TYPE_INDEX = 5;
		public static final int REF_TYPE_INDEX = 6;
		public static final int REF_DOMAIN_INDEX = 7;

		public static String generateRowKey(String date, String webId, Integer visitorType, Integer time, Integer userType, Integer refType, String refDomain){
			StringBuffer sb = new StringBuffer();
			sb.append(generateRowPrefix(date, webId, visitorType));
			sb.append(time).append(RowUtil.ROW_SPLIT);
			sb.append(userType).append(RowUtil.ROW_SPLIT);
			sb.append(refType).append(RowUtil.ROW_SPLIT);
			sb.append(refDomain == null? "" : refDomain);
			return sb.toString();
		}
		
		public static String generateRowPrefix(String date, String webId,Integer visitorType){
			return generateRowPrefix(date, webId) + visitorType + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId){
			return generateRequiredRowPrefix(SIGN_BASIC, date, webId);
		}
	}
	
	/**
	 * 
	 * 文件名：KWRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-15 下午4:50:21
	 * 功能描述：基于搜索词的row生成工具类
	 */
	public static class KWRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int VISITOR_TYPE_INDEX = 3;
		public static final int REF_DOMAIN_INDEX = 4;
		public static final int REF_KEYWORD_INDEX = 5;

		public static String generateRowKey(String date, String webId, Integer visitorType, String refDomain, String refKeyword){
			return generateRowPrefix(date, webId, visitorType, refDomain) + (refKeyword == null? "": refKeyword);
		}

		public static String generateRowPrefix(String date, String webId,Integer visitorType, String refDomain){
			return generateRowPrefix(date, webId, visitorType) + refDomain + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId,Integer visitorType){
			return generateRowPrefix(date, webId) + visitorType + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId){
			return generateRequiredRowPrefix(SIGN_REF_KW, date, webId);
		}
	}
	
	/**
	 * 文件名：EntryPageRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-15 下午4:50:41
	 * 功能描述：基于入口页的row生成工具类
	 */
	public static class EntryPageRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int VISITOR_TYPE_INDEX = 3;
		public static final int PAGE_SIGN_INDEX = 4;

		public static String generateRowKey(String date, String webId, Integer visitorType, String pageSign){
			return generateRowPrefix(date, webId, visitorType) + pageSign;
		}
		
		public static String generateRowPrefix(String date, String webId,Integer visitorType){
			return generateRowPrefix(date, webId) + visitorType + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId){
			return generateRequiredRowPrefix(SIGN_ENTRY_PAGE, date, webId);
		}
	}
	
	/**
	 * 文件名：AreaRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-16 上午10:12:44
	 * 功能描述：基于地域的row生成工具类
	 */
	public static class AreaRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int VISITOR_TYPE_INDEX = 3;
		public static final int CONTRY_ID_INDEX = 4;
		public static final int PROVINCE_ID_INDEX = 5;
		public static final int CITY_ID_INDEX = 6;

		public static String generateRowKey(String date, String webId, Integer visitorType, Integer countryId, Integer provinceId, Integer cityId){
			return generateRowPrefix(date, webId, visitorType, countryId, provinceId) + cityId;
		}
		
		public static String generateRowPrefix(String date, String webId, Integer visitorType, Integer countryId, Integer provinceId){
			return generateRowPrefix(date, webId, visitorType, countryId) + provinceId + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId, Integer visitorType, Integer countryId){
			return generateRowPrefix(date, webId, visitorType) + countryId + RowUtil.ROW_SPLIT ;
		}
		
		public static String generateRowPrefix(String date, String webId,Integer visitorType){
			return generateRowPrefix(date, webId) + visitorType + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId){
			return generateRequiredRowPrefix(SIGN_AREA, date, webId);
		}
	}
	
	/**
	 * 文件名：SysBasicRowGenerator
	 * 创建人：jason.hua
	 * 创建日期：2014-10-15 下午5:07:26
	 * 功能描述：基于系统环境的row生成工具类
	 */
	public static class SysEnvRowGenerator{
		public static final int SIGN_INDEX = 0;
		public static final int DATE_INDEX = 1;
		public static final int WEB_ID_INDEX = 2;
		public static final int VISITOR_TYPE_INDEX = 3;
		public static final int OS_INDEX = 4;
		public static final int BROWSER_INDEX = 5;
		public static final int LANGUAGE_INDEX = 6;
		public static final int IS_ENABLE_COOKIE_INDEX = 7;
		public static final int COLOR_DEPTH_INDEX = 8;
		public static final int SCREEN_INDEX = 9;

		public static String generateRowKey(String date, String webId, Integer visitorType, String os, String browser, String language, 
				Boolean isEnableCookie, String colorDepth, String screen){
			if(os == null) os = "";
			if(browser == null) browser = "";
			if(language == null) 
				language = "";
			else 
				language = language.toLowerCase();
			if(screen == null) screen = "";
			
			String isEnableCookieStr = "";
			if(isEnableCookie != null) isEnableCookieStr = isEnableCookie.toString();
			if(colorDepth == null) colorDepth = "";
			
			StringBuffer sb = new StringBuffer();
			sb.append(generateRowPrefix(date, webId, visitorType));
			sb.append(os).append(RowUtil.ROW_SPLIT);
			sb.append(browser).append(RowUtil.ROW_SPLIT);
			sb.append(language).append(RowUtil.ROW_SPLIT);
			sb.append(isEnableCookieStr).append(RowUtil.ROW_SPLIT);
			sb.append(colorDepth).append(RowUtil.ROW_SPLIT);
			sb.append(screen);
			return sb.toString();
		}
		
		public static String generateRowPrefix(String date, String webId,Integer visitorType){
			return generateRowPrefix(date, webId) + visitorType + RowUtil.ROW_SPLIT;
		}
		
		public static String generateRowPrefix(String date, String webId){
			return generateRequiredRowPrefix(SIGN_SYS_BASIC, date, webId);
		}
	}
	
	/**
	 * 
	 * 函数名：generateRequiredRowPrefix
	 * 功能描述：公共row前缀生成方法
	 * @param sign 标识
	 * @param date 日期YYYYMMDD
	 * @param webId 网站id
	 * @param visitorType 新老访客类型
	 * @return
	 */
	private static String generateRequiredRowPrefix(String sign, String date, String webId){
		StringBuffer sb = new StringBuffer();
		sb.append(sign).append(RowUtil.ROW_SPLIT);
		sb.append(date).append(RowUtil.ROW_SPLIT);
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		return sb.toString();
	}
	
	public Long getPv() {
		return pv;
	}

	public Long getTotalVisitPage() {
		return totalVisitPage;
	}

	public void setTotalVisitPage(Long totalVisitPage) {
		this.totalVisitPage = totalVisitPage;
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

	public Long getTotalJumpCount() {
		return totalJumpCount;
	}

	public void setTotalJumpCount(Long totalJumpCount) {
		this.totalJumpCount = totalJumpCount;
	}


}
