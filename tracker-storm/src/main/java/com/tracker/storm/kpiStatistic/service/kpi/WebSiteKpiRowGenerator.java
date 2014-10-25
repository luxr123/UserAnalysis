package com.tracker.storm.kpiStatistic.service.kpi;

import java.util.ArrayList;
import java.util.List;

import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam.WebSiteRowGenerator;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.db.util.RowUtil;
import com.tracker.storm.kpiStatistic.service.entity.WebSiteKpiDimension;

/**
 * pv, uv, ip数统计
 * @author jason.hua
 *
 */
public class WebSiteKpiRowGenerator {
	
	/**
	 * 网站可累加kpi
	 * (日期，小时段，来源，地域，系统环境)
	 */
	public static List<String> getWebSiteSummableKpiRows(WebSiteKpiDimension kpiDimesion){
		List<String> rows = new ArrayList<String>();
		Integer visitorType = kpiDimesion.getVisitorType();
		String webId = kpiDimesion.getWebId();
		String date  = kpiDimesion.getDate();
		int hour = kpiDimesion.getHour();
		
		//basic，基于日期、角色、小时段
		rows.add(WebSiteSummableKpi.BasicRowGenerator.generateRowKey(date, webId, visitorType, hour, kpiDimesion.getUserType()));
		
		//area, 国家、省份、市
		rows.add(WebSiteSummableKpi.AreaRowGenerator.generateRowKey(date, webId, visitorType, 
				kpiDimesion.getCountryId(), kpiDimesion.getProvinceId(), kpiDimesion.getCityId()));
		
		//ref， 基于来源类型、来源域名、外部搜索关键词
		rows.add(WebSiteSummableKpi.RefRowGenerator.generateRowKey(date, webId, visitorType, kpiDimesion.getRefType(), kpiDimesion.getRefDomain(), kpiDimesion.getRefKeyword()));
	
		//sys-basic， 基于操作系统、浏览器、语言环境、是否支持cookie、屏幕颜色
		rows.add(WebSiteSummableKpi.SysBasicRowGenerator.generateRowKey(date, webId, visitorType, kpiDimesion.getOs(), 
				kpiDimesion.getBrowser(), kpiDimesion.getLanguage(), kpiDimesion.getIsCookieEnabled(), kpiDimesion.getColorDepth()));
		
		//sys-screen， 基于屏幕分辩率
		if(kpiDimesion.getScreen() != null)
			rows.add(WebSiteSummableKpi.SysScreenRowGenerator.generateRowKey(date, webId, visitorType, kpiDimesion.getScreen()));
		return rows;
	}
	
	/**
	 * 基于入口页可累加kpi
	 */
	public static String getEntryPageSummableKpiRow(String date, String webId, String entryPageSign, Integer visitorType){
		return WebSiteSummableKpi.EntryPageRowGenerator.generateRowKey(date, webId, visitorType, entryPageSign);
	}
	
	/**
	 * 基于受访页可累加kpi
	 */
	public static String getPageSummableKpiRow(String date, String webId, String pageSign, Integer visitorType){
		return PageSummableKpi.generateRowKey(date, webId, visitorType, pageSign);
	}
	
	/**
	 * 网站不可累加kpi
	 * (日期，小时段，来源，地域，页面, 系统环境)
	 */
	public static List<String> getUnSummableKpiRowsForAll(String kpi, Integer visitorType, WebSiteKpiDimension kpiDimesion, String ipOrCookieId){
		String date = kpiDimesion.getDate();
		String webId = kpiDimesion.getWebId();
		String pageSign = kpiDimesion.getPageSign();
		
		List<String> rowList = new ArrayList<String>();

		//日期、小时段、页面
		rowList.addAll(getUnSummableKpiRowsForBasic(kpi, date, visitorType, webId, pageSign, kpiDimesion.getHour(), ipOrCookieId));
		
		//入口页
		rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, kpi, date, webId, visitorType, pageSign, ipOrCookieId));
		
		//来源
		int refType = kpiDimesion.getRefType();
		String refDomain = kpiDimesion.getRefDomain();
		String refKeyword = kpiDimesion.getRefKeyword();
		rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF, kpi, date, webId, visitorType, refType + "", ipOrCookieId));
		if(refDomain != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, kpi, date, webId, visitorType, refDomain, ipOrCookieId));
			if(refType == ReferrerType.SEARCH_ENGINE.getValue() && refKeyword != null){
				rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + refDomain, kpi, date, webId, visitorType, refKeyword, ipOrCookieId));
				rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + "", kpi, date, webId, visitorType, refKeyword, ipOrCookieId));
			}
		}
		
		//地域
		Integer countryId = kpiDimesion.getCountryId();
		Integer provinceId = kpiDimesion.getProvinceId();
		Integer cityId = kpiDimesion.getCityId();
		if(countryId != null && provinceId != null && cityId != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + countryId, kpi, date, webId, visitorType, provinceId + "", ipOrCookieId));
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + countryId + RowUtil.FIELD_SPLIT + provinceId, kpi, date, webId, visitorType, cityId + "", ipOrCookieId));
		}

		//系统环境
		if(kpiDimesion.getBrowser() != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.BROWSER.getValue(), kpi, date, webId, visitorType, kpiDimesion.getBrowser(), ipOrCookieId));
		}
		if(kpiDimesion.getOs() != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.OS.getValue(), kpi, date, webId, visitorType, kpiDimesion.getOs(), ipOrCookieId));
		}
		if(kpiDimesion.getColorDepth() != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COLOR_DEPTH.getValue(), kpi, date, webId, visitorType, kpiDimesion.getColorDepth(), ipOrCookieId));
		}
		if(kpiDimesion.getIsCookieEnabled() != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COOKIE_ENABLED.getValue(), kpi, date, webId, visitorType, kpiDimesion.getIsCookieEnabled().toString(), ipOrCookieId));
		}
		if(kpiDimesion.getLanguage() != null){
			String language = kpiDimesion.getLanguage().toLowerCase();
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.LANGUAGE.getValue(), kpi, date, webId, visitorType, language, ipOrCookieId));
		}
		if(kpiDimesion.getScreen() != null){
			rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.SCREEN.getValue(), kpi, date, webId, visitorType, kpiDimesion.getScreen(), ipOrCookieId));
		}
		return rowList;
	}
	
	/**
	 * 
	 * 函数名：getUnSummableKpiRowForBasic
	 * 功能描述：
	 * @param kpi
	 * @param date
	 * @param visitorType
	 * @param webId
	 * @param pageSign
	 * @param hour
	 * @param userType
	 * @param ipOrCookieId
	 * @return
	 */
	public static List<String> getUnSummableKpiRowsForBasic(String kpi, String date, Integer visitorType, 
			String webId, String pageSign, int hour, String ipOrCookieId){
		List<String> rowList = new ArrayList<String>();
		//日期
		rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_DATE, kpi, date, webId, visitorType, date, ipOrCookieId));
		//小时段
		rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_TIME, kpi, date, webId, visitorType, hour + "", ipOrCookieId));
		//页面
		rowList.add(WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PAGE, kpi, date, webId, visitorType, pageSign, ipOrCookieId));
		return rowList;
	}
	
	/**
	 * 
	 * 函数名：getUnSummableKpiRowsForUser
	 * 功能描述：
	 * @param kpi
	 * @param date
	 * @param visitorType
	 * @param webId
	 * @param pageSign
	 * @param hour
	 * @param userType
	 * @param ipOrCookieId
	 * @return
	 */
	public static String getUnSummableKpiRowsForUser(String kpi, String date, Integer visitorType, 
			String webId, int userType, String ipOrCookieId){
		return WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_USER, kpi, date, webId, visitorType, userType + "", ipOrCookieId);
	}
	
}	
