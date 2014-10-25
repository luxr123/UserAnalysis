package com.tracker.hbase.service.run;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.base.Function;
import com.tracker.common.cache.LocalCache;
import com.tracker.common.constant.search.SearchResultType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.common.constant.website.VisitorType;
import com.tracker.db.constants.AreaLevel;
import com.tracker.db.constants.DateType;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.Geography;
import com.tracker.db.dao.data.model.SiteSearchEngine;
import com.tracker.db.dao.data.model.SiteSearchType;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;
import com.tracker.hbase.service.sitesearch.SiteSearchCount;
import com.tracker.hbase.service.sitesearch.SiteSearchCountImpl;
import com.tracker.hbase.service.util.HbaseUtil;
import com.tracker.hbase.service.website.WebSiteCount;
import com.tracker.hbase.service.website.WebSiteCountImpl;

/**
 * 文件名：Count
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:16:59
 * 功能描述：按周、月、年对Hbase中的数据进行统计，并将统计结果存入Hbase
 *
 */
public class Count {
	private static WebSiteCount webSiteCount = new WebSiteCountImpl();
	private static SiteSearchCount siteSearchCount = new SiteSearchCountImpl();
	
	private static String week = null;
	private static String month = null; 
	private static String year = null;
		
	/**
	 * 三种执行方式：
	 * 1、只传入0，执行所有业务
	 * 2、传入1和某个业务名称（业务对应的方法名），执行此业务以及其后的所有业务
	 * 3、传入2和某个业务名称，只执行此业务
	 * @param args
	 */
	
	 /**
	  * 所有业务（名称）以及执行顺序
		countWebStatsByHour
		countWebStats
		countWebStatsForRefType
		countWebStatsForProvince
		countWebStatsForCity
		countWebStatsForPage
		countWebStatsForEntryPage
		countWebStatsForSysEnv
		
		countSearchStats
		countSearchResultStats
		countSearchConditionStats
	**/
	
	public static void main(String[] args) {
		Calendar calendar = null; 
		SimpleDateFormat sdf = null;
		
		calendar = Calendar.getInstance();
		sdf = new SimpleDateFormat("yyyyMMdd");
		String today = sdf.format(calendar.getTime());
		
		//周
	    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
	    	calendar.add(Calendar.WEEK_OF_YEAR, -1);
		}	    
		sdf = new SimpleDateFormat("yyyyww");
		week = sdf.format(calendar.getTime()); 
		
		//月
		calendar = Calendar.getInstance();
		if (Integer.parseInt(today.substring(6)) == 1) {
			calendar.add(Calendar.MONTH, -1); 
		}
	    sdf = new SimpleDateFormat("yyyyMM");
	    month = sdf.format(calendar.getTime()); 
	    
		//年
	    calendar = Calendar.getInstance();
		if (Integer.parseInt(today.substring(4)) == 101) {
			calendar.add(Calendar.YEAR, -1);
		}
	    sdf = new SimpleDateFormat("yyyy");
	    year = sdf.format(calendar.getTime()); 
	    
	    
		Integer runType = Integer.valueOf(args[0]);
        if (runType == 0) {
        	// 执行所有业务
			business_oneOrMore("countWebUserStats");
		} else {
			String business = args[1];
			if (runType == 1) {
				// 执行business以及其后的所有业务
				business_oneOrMore(business);
			} else if (runType == 2) {
				// 执行business业务
				business_one(business);
			}
		}
        
       // 关闭Hbase连接
       HbaseUtil.shutdownHConnection();
	}
	
	/**
	 * 一次执行一个业务
	 * @param business
	 */
	private static void business_one(String business) {
		Map<Integer, List<Integer>> idAndSearchTypes = null;
		switch (Business.getBusiness(business)) {
			case countWebUserStats:
				webSiteCount.countWebUserStats(1, DateType.WEEK.getValue(), week);
				
				webSiteCount.countWebUserStats(1, DateType.MONTH.getValue(), month);
				
				webSiteCount.countWebUserStats(1, DateType.YEAR.getValue(), year);
				break;      
			case countWebStatsByHour:
				webSiteCount.countWebStatsByHour(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsByHour(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsByHour(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.YEAR.getValue(), year, null);
				break;
			case countWebStats:
				webSiteCount.countWebStats(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStats(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStats(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.YEAR.getValue(), year, null);
				break;
			case countWebStatsForRefType:
				webSiteCount.countWebStatsForRefType(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsForRefType(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsForRefType(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.YEAR.getValue(), year, null);
				break;
			case countWebStatsForProvince:
				webSiteCount.countWebStatsForProvince(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.WEEK.getValue(), week, null, 1);
				
				webSiteCount.countWebStatsForProvince(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.MONTH.getValue(), month, null, 1);
				
				webSiteCount.countWebStatsForProvince(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.YEAR.getValue(), year, null, 1);
				break;
			case countWebStatsForCity:
				Collection<Integer> provinceIdList = getProvinceMap(1).keySet();
				for(Integer provinceId: provinceIdList){ 
					webSiteCount.countWebStatsForCity(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.WEEK.getValue(), week, null, 1, provinceId);
					
					webSiteCount.countWebStatsForCity(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.MONTH.getValue(), month, null, 1, provinceId);
					
					webSiteCount.countWebStatsForCity(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.YEAR.getValue(), year, null, 1, provinceId);
				}
				break;
			case countWebStatsForPage:
				webSiteCount.countWebStatsForPage(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsForPage(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsForPage(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.YEAR.getValue(), year, null);
				break;
			case countWebStatsForEntryPage:
				webSiteCount.countWebStatsForEntryPage(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsForEntryPage(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsForEntryPage(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.YEAR.getValue(), year, null);
				break;
			case countWebStatsForSysEnv:
		        SysEnvType[] types = SysEnvType.values();
		        for (int i = 0; i < types.length; i++) {
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.WEEK.getValue(), week, types[i].getValue(), VisitorType.NEW_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.WEEK.getValue(), week, types[i].getValue(), VisitorType.OLD_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.WEEK.getValue(), week, types[i].getValue(), null);
		        	
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.MONTH.getValue(), month, types[i].getValue(), VisitorType.NEW_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.MONTH.getValue(), month, types[i].getValue(), VisitorType.OLD_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.MONTH.getValue(), month, types[i].getValue(), null);
		        	
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.YEAR.getValue(), year, types[i].getValue(), VisitorType.NEW_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.YEAR.getValue(), year, types[i].getValue(), VisitorType.OLD_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.YEAR.getValue(), year, types[i].getValue(), null);
				}
				break;
			case countSearchStats:
				idAndSearchTypes = getSearchIdAndSearchTypes();
				for (Integer searchId : idAndSearchTypes.keySet()) {
					List<Integer> searchTypes = idAndSearchTypes.get(searchId);
					for (Integer searchType : searchTypes) {
						siteSearchCount.countSearchStats(1, DateType.WEEK.getValue(), week, searchId, searchType);
						
						siteSearchCount.countSearchStats(1, DateType.MONTH.getValue(), month, searchId, searchType);
						
						siteSearchCount.countSearchStats(1, DateType.YEAR.getValue(), year, searchId, searchType);
					}
				}
				break;
			case countSearchResultStats:
				idAndSearchTypes = getSearchIdAndSearchTypes();
				for (Integer searchId : idAndSearchTypes.keySet()) {
					List<Integer> searchTypes = idAndSearchTypes.get(searchId);
					for (Integer searchType : searchTypes) {
					    SearchResultType[] tps = SearchResultType.values();
					    for (SearchResultType searchResultType : tps) {
					    	siteSearchCount.countSearchResultStats(1, DateType.WEEK.getValue(), week, searchId, searchType, searchResultType.getType());
					    	
					    	siteSearchCount.countSearchResultStats(1, DateType.MONTH.getValue(), month, searchId, searchType, searchResultType.getType());
					    	
					    	siteSearchCount.countSearchResultStats(1, DateType.YEAR.getValue(), year, searchId, searchType, searchResultType.getType());
						}
					}
				}
				break;
			case countSearchConditionStats:
				idAndSearchTypes = getSearchIdAndSearchTypes();
				for (Integer searchId : idAndSearchTypes.keySet()) {
					List<Integer> searchTypes = idAndSearchTypes.get(searchId);
					for (Integer searchType : searchTypes) {
						siteSearchCount.countSearchConditionStats(1, DateType.WEEK.getValue(), week, searchId, searchType);
						
						siteSearchCount.countSearchConditionStats(1, DateType.MONTH.getValue(), month, searchId, searchType);
						
						siteSearchCount.countSearchConditionStats(1, DateType.YEAR.getValue(), year, searchId, searchType);
					}
				}
				break;
		}
	}
	
	/**
	 * 一次执行一个或多个业务	
	 * @param business
	 */
	private static void business_oneOrMore(String business) {
		Map<Integer, List<Integer>> idAndSearchTypes = null;
		switch (Business.getBusiness(business)) {
			case countWebUserStats:
				webSiteCount.countWebUserStats(1, DateType.WEEK.getValue(), week);
				
				webSiteCount.countWebUserStats(1, DateType.MONTH.getValue(), month);
				
				webSiteCount.countWebUserStats(1, DateType.YEAR.getValue(), year);
			case countWebStatsByHour:
				webSiteCount.countWebStatsByHour(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsByHour(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsByHour(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsByHour(1, DateType.YEAR.getValue(), year, null);
			case countWebStats:
				webSiteCount.countWebStats(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStats(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStats(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStats(1, DateType.YEAR.getValue(), year, null);
			case countWebStatsForRefType:
				webSiteCount.countWebStatsForRefType(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsForRefType(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsForRefType(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForRefType(1, DateType.YEAR.getValue(), year, null);
			case countWebStatsForProvince:
				webSiteCount.countWebStatsForProvince(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.WEEK.getValue(), week, null, 1);
				
				webSiteCount.countWebStatsForProvince(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.MONTH.getValue(), month, null, 1);
				
				webSiteCount.countWebStatsForProvince(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue(), 1);
				webSiteCount.countWebStatsForProvince(1, DateType.YEAR.getValue(), year, null, 1);
			case countWebStatsForCity:
				Collection<Integer> provinceIdList = getProvinceMap(1).keySet();
				for(Integer provinceId: provinceIdList){ 
					webSiteCount.countWebStatsForCity(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.WEEK.getValue(), week, null, 1, provinceId);
					
					webSiteCount.countWebStatsForCity(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.MONTH.getValue(), month, null, 1, provinceId);
					
					webSiteCount.countWebStatsForCity(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue(), 1, provinceId);
					webSiteCount.countWebStatsForCity(1, DateType.YEAR.getValue(), year, null, 1, provinceId);
				}
			case countWebStatsForPage:
				webSiteCount.countWebStatsForPage(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsForPage(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsForPage(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForPage(1, DateType.YEAR.getValue(), year, null);
			case countWebStatsForEntryPage:
				webSiteCount.countWebStatsForEntryPage(1, DateType.WEEK.getValue(), week, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.WEEK.getValue(), week, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.WEEK.getValue(), week, null);
				
				webSiteCount.countWebStatsForEntryPage(1, DateType.MONTH.getValue(), month, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.MONTH.getValue(), month, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.MONTH.getValue(), month, null);
				
				webSiteCount.countWebStatsForEntryPage(1, DateType.YEAR.getValue(), year, VisitorType.NEW_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.YEAR.getValue(), year, VisitorType.OLD_VISITOR.getValue());
				webSiteCount.countWebStatsForEntryPage(1, DateType.YEAR.getValue(), year, null);
			case countWebStatsForSysEnv:
		        SysEnvType[] types = SysEnvType.values();
		        for (int i = 0; i < types.length; i++) {
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.WEEK.getValue(), week, types[i].getValue(), VisitorType.NEW_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.WEEK.getValue(), week, types[i].getValue(), VisitorType.OLD_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.WEEK.getValue(), week, types[i].getValue(), null);
		        	
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.MONTH.getValue(), month, types[i].getValue(), VisitorType.NEW_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.MONTH.getValue(), month, types[i].getValue(), VisitorType.OLD_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.MONTH.getValue(), month, types[i].getValue(), null);
		        	
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.YEAR.getValue(), year, types[i].getValue(), VisitorType.NEW_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.YEAR.getValue(), year, types[i].getValue(), VisitorType.OLD_VISITOR.getValue());
		        	webSiteCount.countWebStatsForSysEnv(1, DateType.YEAR.getValue(), year, types[i].getValue(), null);
				}
			case countSearchStats:
				idAndSearchTypes = getSearchIdAndSearchTypes();
				for (Integer searchId : idAndSearchTypes.keySet()) {
					List<Integer> searchTypes = idAndSearchTypes.get(searchId);
					for (Integer searchType : searchTypes) {
						siteSearchCount.countSearchStats(1, DateType.WEEK.getValue(), week, searchId, searchType);
						
						siteSearchCount.countSearchStats(1, DateType.MONTH.getValue(), month, searchId, searchType);
						
						siteSearchCount.countSearchStats(1, DateType.YEAR.getValue(), year, searchId, searchType);
					}
				}
			case countSearchResultStats:
				idAndSearchTypes = getSearchIdAndSearchTypes();
				for (Integer searchId : idAndSearchTypes.keySet()) {
					List<Integer> searchTypes = idAndSearchTypes.get(searchId);
					for (Integer searchType : searchTypes) {
					    SearchResultType[] tps = SearchResultType.values();
					    for (SearchResultType searchResultType : tps) {
					    	siteSearchCount.countSearchResultStats(1, DateType.WEEK.getValue(), week, searchId, searchType, searchResultType.getType());
					    	
					    	siteSearchCount.countSearchResultStats(1, DateType.MONTH.getValue(), month, searchId, searchType, searchResultType.getType());
					    	
					    	siteSearchCount.countSearchResultStats(1, DateType.YEAR.getValue(), year, searchId, searchType, searchResultType.getType());
						}
					}
				}
			case countSearchConditionStats:
				idAndSearchTypes = getSearchIdAndSearchTypes();
				for (Integer searchId : idAndSearchTypes.keySet()) {
					List<Integer> searchTypes = idAndSearchTypes.get(searchId);
					for (Integer searchType : searchTypes) {
						siteSearchCount.countSearchConditionStats(1, DateType.WEEK.getValue(), week, searchId, searchType);
						
						siteSearchCount.countSearchConditionStats(1, DateType.MONTH.getValue(), month, searchId, searchType);
						
						siteSearchCount.countSearchConditionStats(1, DateType.YEAR.getValue(), year, searchId, searchType);
					}
				}
				break;
		}
	}
	
	
	
	/**
	 * 获取省份数据集合
	 */
	public static Map<Integer, String> getProvinceMap(final int countryId){
		LocalCache<String, Object> cache = new LocalCache<String, Object>(3600); // 缓存1个小时
		Object value = cache.getOrElse("province_" + countryId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.PROVINCE.getValue());
				QueryExtInfo queryExtInfo = new QueryExtInfo();
				Geography geography = new Geography();
				geography.setCountryId(countryId);
				queryExtInfo.setObj(geography);
				
				HBaseDao geographyDao = new HBaseDao(HbaseUtil.getHConnection(), Geography.class);
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geographyDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, queryExtInfo);
				Map<Integer, String> result = new HashMap<Integer, String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
						result.put(RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX), "");
					}
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	
	
	/**
	 * 获取searchId和对应的searchType
	 * @return
	 */
	private static Map<Integer, List<Integer>> getSearchIdAndSearchTypes() {
		Map<Integer, List<Integer>> idAndSearchTypes = new HashMap<Integer, List<Integer>>();
		
		LocalCache<String, Object> cache = new LocalCache<String, Object>(60 * 60);
		Object value = cache.getOrElse("siteSE_map<id, name>", new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				HBaseDao siteSEDao = new HBaseDao(HbaseUtil.getHConnection(), SiteSearchEngine.class);
				List<SimpleHbaseDOWithKeyResult<SiteSearchEngine>> rowObjList = siteSEDao.findObjectListAndKeyByRowPrefix(SiteSearchEngine.generateRowPrefix(), SiteSearchEngine.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchEngine> rowObj: rowObjList){
					int seId = RowUtil.getRowIntField(rowObj.getRowKey(), SiteSearchEngine.SE_ID_INDEX);
					result.put(seId, "");
				}
				return result;
			}
		});
		Map<Integer, String> map =  (Map<Integer, String>)value;
		
		// 根据searchId得到相应的searchType
		for(Integer searchId: map.keySet()){
			Map<Integer, String> map_1 = getSiteSearchType(searchId);
			List<Integer> searchTypes = new ArrayList<Integer>();
			for(Integer searchType: map_1.keySet()){
				searchTypes.add(searchType);
			}
			idAndSearchTypes.put(searchId, searchTypes);
		}
		return idAndSearchTypes;
	}
	
	
	/**
	 * 获取搜索类型
	 */
	public static Map<Integer, String> getSiteSearchType(final int siteSeId) {
		LocalCache<String, Object> cache = new LocalCache<String, Object>(60 * 60);
		Object value = cache.getOrElse("siteSearchType_map<id, desc>" + siteSeId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				HBaseDao siteSETypeDao = new HBaseDao(HbaseUtil.getHConnection(), SiteSearchType.class);
				List<SimpleHbaseDOWithKeyResult<SiteSearchType>> rowObjList = siteSETypeDao.findObjectListAndKeyByRowPrefix(SiteSearchType.generateRowPrefix(siteSeId), SiteSearchType.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchType> rowObj: rowObjList){
					int seType = RowUtil.getRowIntField(rowObj.getRowKey(), SiteSearchType.SE_TYPE_INDEX);
					result.put(seType, "");
				}
				return result;
			}
		});
		
		return (Map<Integer, String>)value;
	}
}
