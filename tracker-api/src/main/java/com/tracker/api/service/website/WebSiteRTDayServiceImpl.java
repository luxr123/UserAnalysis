package com.tracker.api.service.website;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import com.tracker.api.Servers;
import com.tracker.api.thrift.web.EntryPageStats;
import com.tracker.api.thrift.web.PageStats;
import com.tracker.api.thrift.web.WebStats;
import com.tracker.api.util.NumericUtil;
import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.common.constant.website.VisitorType;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.db.util.RowUtil;

/**
 * 实时可累加指标
 * @author jason.hua
 *
 */
public class WebSiteRTDayServiceImpl implements WebSiteService{
	private SummableKpiDao summableKpiDao = new SummableKpiHBaseDaoImpl(Servers.hbaseConnection);
	private UnSummableKpiDao unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(Servers.hbaseConnection, UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_DAY_TABLE);

	/**
	 * 获取基于用户类型的统计指标
	 */
	@Override
	public Map<String, WebStats> getWebUserStats(Integer webId,
			Integer timeType, String time) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		String rowPrefix = WebSiteSummableKpi.BasicRowGenerator.generateRowPrefix(time, String.valueOf(webId));
		Map<String, Long> pvMap = summableKpiDao.getWebSitePVKpi(Lists.newArrayList(rowPrefix), WebSiteSummableKpi.BasicRowGenerator.USER_TYPE_INDEX);
		
		CountDownLatch allDone = new CountDownLatch(2);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_USER, null, new ArrayList<String>(pvMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_USER, null, new ArrayList<String>(pvMap.keySet()));
		
		try {
			allDone.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for(String userType: pvMap.keySet()){
			WebStats stats = new WebStats();
			Long pv = pvMap.get(userType);
			Long ip = ipMap.get(userType);
			Long uv = uvMap.get(userType);
			stats.setPv(pv == null? 0: pv);
			stats.setIpCount(ip == null? 0: ip);
			stats.setUv(uv == null? 0: uv);
			result.put(userType, stats);
		}
		return result;
	}

	/**
	 * 获取基于小时段的统计指标
	 */
	@Override
	public Map<String, WebStats> getWebStatsByHour(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		String rowPrefix = null;
		if(visitorType == null)
			rowPrefix = WebSiteSummableKpi.BasicRowGenerator.generateRowPrefix(time, String.valueOf(webId));
		else 
			rowPrefix = WebSiteSummableKpi.BasicRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType);

		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteKpi(Lists.newArrayList(rowPrefix), WebSiteSummableKpi.BasicRowGenerator.TIME_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_TIME, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_TIME, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		//hour
		for(String hour: kpiResultMap.keySet()){
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(hour), ipMap.get(hour), uvMap.get(hour));
			stats.setShowName(hour);
			result.put(hour, stats);
		}
		return result;
	}
	
	/**
	 * 获取基于日期的统计指标
	 */
	@Override
	public Map<String, WebStats> getWebStatsByDates(Integer webId, Integer timeType,
			List<String> times, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		if(times == null || times.size() == 0)
			return result;
		String time = times.get(0);
		String rowPrefix = null;
		if(visitorType == null)
			rowPrefix = WebSiteSummableKpi.BasicRowGenerator.generateRowPrefix(time, String.valueOf(webId));
		else 
			rowPrefix = WebSiteSummableKpi.BasicRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType);
		
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteKpi(Lists.newArrayList(rowPrefix), WebSiteSummableKpi.BasicRowGenerator.DATE_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_DATE, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_DATE, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		WebStats stats = new WebStats();
		fillSummableKpiForWebStats(stats, kpiResultMap.get(time), ipMap.get(time), uvMap.get(time));
		stats.setShowName(time);
		result.put(time, stats);
		return result;
	}

	/**
	 * 获取基于访问来源类型的统计指标
	 */ 
	public Map<String, WebStats> getWebStatsForRefType(Integer webId, Integer timeType, String time, Integer visitorType){
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		
		String rowPrefix = null;
		if(visitorType == null)
			rowPrefix = WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId));
		else 
			rowPrefix = WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType);
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteKpi(Lists.newArrayList(rowPrefix), WebSiteSummableKpi.RefRowGenerator.REF_TYPE_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		
		for(String refType: kpiResultMap.keySet()){ 
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(refType), ipMap.get(refType), uvMap.get(refType));
			stats.setShowName(refType);
			result.put(refType, stats);
		}
		return result;
	}
	
	@Override
	public Map<String, WebStats> getWebStatsForRefDomain(Integer webId,
			Integer timeType, String time, Integer refType,
			Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		List<String> rowPrefixList = null;
		if(visitorType == null){
			rowPrefixList = Lists.newArrayList(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.NEW_VISITOR.getValue(), refType));
			rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.OLD_VISITOR.getValue(), refType));
		} else {
			rowPrefixList = Lists.newArrayList(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType, refType));
		}
		
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteTopKpi(rowPrefixList, WebSiteSummableKpi.RefRowGenerator.REF_DOMAIN_INDEX, topNum);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		for(String refDomain: kpiResultMap.keySet()){ 
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(refDomain), ipMap.get(refDomain), uvMap.get(refDomain));
			stats.setShowName(refDomain);
			result.put(refDomain, stats);
		}
		return result;
	}
	
	/**
	 * 基于搜索词获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForKeyword(Integer webId, Integer timeType,
			String time, String seDomain, Integer visitorType, Integer topNum){
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		List<String> rowPrefixList = new ArrayList<String>();
		if(visitorType == null){
			if(seDomain == null){
				rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.NEW_VISITOR.getValue(), ReferrerType.SEARCH_ENGINE.getValue()));
				rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.OLD_VISITOR.getValue(), ReferrerType.SEARCH_ENGINE.getValue()));
			} else {
				rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.NEW_VISITOR.getValue(), ReferrerType.SEARCH_ENGINE.getValue(), seDomain));
				rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.OLD_VISITOR.getValue(), ReferrerType.SEARCH_ENGINE.getValue(), seDomain));
			}
		} else {
			if(seDomain == null){
				rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType, ReferrerType.SEARCH_ENGINE.getValue()));
			} else {
				rowPrefixList.add(WebSiteSummableKpi.RefRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType, ReferrerType.SEARCH_ENGINE.getValue(), seDomain));
			}
		}
		
		if(seDomain == null) seDomain = "";
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteTopKpi(rowPrefixList, WebSiteSummableKpi.RefRowGenerator.REF_KEYWORD_INDEX, topNum);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + seDomain, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + seDomain, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		for(String keyword: kpiResultMap.keySet()){ 
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(keyword), ipMap.get(keyword), uvMap.get(keyword));
			stats.setShowName(keyword);
			result.put(keyword, stats);
		}
		return result;
	}
	
	/**
	 * 基于省份获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForProvince(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		List<String> rowPrefixList = new ArrayList<String>();
		if(visitorType == null){
			rowPrefixList.add(WebSiteSummableKpi.AreaRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.NEW_VISITOR.getValue(), countryId));
			rowPrefixList.add(WebSiteSummableKpi.AreaRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.OLD_VISITOR.getValue(), countryId));
		} else {
			rowPrefixList.add(WebSiteSummableKpi.AreaRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType, countryId));
		}
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteKpi(rowPrefixList, WebSiteSummableKpi.AreaRowGenerator.PROVINCE_ID_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + countryId, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + countryId, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		//provinceId
		for(String provinceId: kpiResultMap.keySet()){ 
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(provinceId), ipMap.get(provinceId), uvMap.get(provinceId));
			result.put(provinceId, stats);
		}
		return result;
	}
	
	/**
	 * 基于城市获取统计指标
	 */
	@Override
	public Map<String, WebStats> getWebStatsForCity(Integer webId,
			Integer timeType, String time, Integer visitorType, Integer countryId, Integer provinceId) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		
		List<String> rowPrefixList = new ArrayList<String>();
		if(visitorType == null){
			rowPrefixList.add(WebSiteSummableKpi.AreaRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.NEW_VISITOR.getValue(), countryId, provinceId));
			rowPrefixList.add(WebSiteSummableKpi.AreaRowGenerator.generateRowPrefix(time, String.valueOf(webId), VisitorType.OLD_VISITOR.getValue(), countryId, provinceId));
		} else {
			rowPrefixList.add(WebSiteSummableKpi.AreaRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType, countryId, provinceId));
		}
		
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteKpi(rowPrefixList, WebSiteSummableKpi.AreaRowGenerator.CITY_ID_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + countryId + RowUtil.FIELD_SPLIT + provinceId, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + countryId + RowUtil.FIELD_SPLIT + provinceId, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		//cityId
		for(String cityId: kpiResultMap.keySet()){ 
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(cityId), ipMap.get(cityId), uvMap.get(cityId));
			result.put(cityId, stats);
		}
		return result;
	}
	
	/**
	 * 获取基于访问页数统计指标
	 */
	public Map<String, PageStats> getWebStatsForPage(Integer webId, Integer timeType, String time, Integer visitorType)  {
		Map<String, PageStats> result = new HashMap<String, PageStats>();
		
		List<String> rowPrefixList = new ArrayList<String>();
		if(visitorType == null){
			rowPrefixList.add(PageSummableKpi.generateRowPrefix(time, String.valueOf(webId)));
		} else {
			rowPrefixList.add(PageSummableKpi.generateRowPrefix(time, String.valueOf(webId), visitorType));
		}
		
		Map<String, PageSummableKpi> kpiResultMap = summableKpiDao.getWebSitePageKpi(rowPrefixList, PageSummableKpi.PAGE_SIGN_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PAGE, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PAGE, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		for(String pageSign: kpiResultMap.keySet()){
			PageStats stats = new PageStats();
			PageSummableKpi kpiResult = kpiResultMap.get(pageSign);
			stats.setPv(kpiResult.getPv());
            stats.setEntryPageCount(kpiResult.getEntryPageCount());
			stats.setNextPageCount(kpiResult.getNextPageCount());
			stats.setOutPageCount(kpiResult.getOutPageCount());
			stats.setAvgStayTime(NumericUtil.getRate(kpiResult.getStayTime(), kpiResult.getPv()));
			stats.setOutRate(NumericUtil.getRate(kpiResult.getOutPageCount(), kpiResult.getPv()));
			if(ipMap.containsKey(pageSign)) stats.setIpCount(ipMap.get(pageSign));
			if(uvMap.containsKey(pageSign)) stats.setUv(uvMap.get(pageSign));
			result.put(pageSign, stats); 
		}
		return result;
	}

	/**
	 * 获取基于入口页的统计指标
	 */
	public Map<String, EntryPageStats> getWebStatsForEntryPage(Integer webId,
			Integer timeType, String time, Integer visitorType) {
		Map<String, EntryPageStats> result = new HashMap<String, EntryPageStats>();
		
		List<String> rowPrefixList = new ArrayList<String>();
		if(visitorType == null){
			rowPrefixList.add(WebSiteSummableKpi.EntryPageRowGenerator.generateRowPrefix(time, String.valueOf(webId)));
		} else {
			rowPrefixList.add(WebSiteSummableKpi.EntryPageRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType));
		}
		
		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteKpi(rowPrefixList, WebSiteSummableKpi.EntryPageRowGenerator.PAGE_SIGN_INDEX);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		
		for(String pageSign: kpiResultMap.keySet()){
			EntryPageStats stats = new EntryPageStats();
			WebSiteSummableKpi kpiResult = kpiResultMap.get(pageSign);
			stats.setPv(kpiResult.getPv());
			stats.setVisitTimes(kpiResult.getVisitTimes());
			stats.setAvgVisitTime(NumericUtil.getRate(kpiResult.getTotalVisitTime(), kpiResult.getVisitTimes()));
			stats.setAvgVisitPage(NumericUtil.getRate(kpiResult.getTotalVisitPage(), kpiResult.getVisitTimes()));
			stats.setJumpRate(NumericUtil.getRate(kpiResult.getTotalJumpCount(), kpiResult.getVisitTimes()));
			if(ipMap.containsKey(pageSign)) stats.setIpCount(ipMap.get(pageSign));
			if(uvMap.containsKey(pageSign)) stats.setUv(uvMap.get(pageSign));
			result.put(pageSign, stats); 
		}
		return result;
	}
	
	/**
	 * 获取基于系统环境的统计指标
	 */
	public Map<String, WebStats> getWebStatsForSysEnv(Integer webId, Integer timeType,
			String time, Integer sysType, Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		
		List<String> rowPrefixList = new ArrayList<String>();
		if(sysType == SysEnvType.SCREEN.getValue()){
			if(visitorType == null){
				rowPrefixList.add(WebSiteSummableKpi.SysScreenRowGenerator.generateRowPrefix(time, String.valueOf(webId)));
			} else {
				rowPrefixList.add(WebSiteSummableKpi.SysScreenRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType));
			}
		} else {
			if(visitorType == null){
				rowPrefixList.add(WebSiteSummableKpi.SysBasicRowGenerator.generateRowPrefix(time, String.valueOf(webId)));
			} else {
				rowPrefixList.add(WebSiteSummableKpi.SysBasicRowGenerator.generateRowPrefix(time, String.valueOf(webId), visitorType));
			}
		}
		int fieldIndex = 0;
		if(sysType == SysEnvType.SCREEN.getValue()) 
			fieldIndex = WebSiteSummableKpi.SysScreenRowGenerator.SCREEN_INDEX;
		else if(sysType == SysEnvType.BROWSER.getValue()) 
			fieldIndex = WebSiteSummableKpi.SysBasicRowGenerator.BROWSER_INDEX;
		else if(sysType == SysEnvType.COLOR_DEPTH.getValue()) 
			fieldIndex = WebSiteSummableKpi.SysBasicRowGenerator.COLOR_DEPTH_INDEX;
		else if(sysType == SysEnvType.COOKIE_ENABLED.getValue()) 
			fieldIndex = WebSiteSummableKpi.SysBasicRowGenerator.IS_ENABLE_COOKIE_INDEX;
		else if(sysType == SysEnvType.LANGUAGE.getValue()) 
			fieldIndex = WebSiteSummableKpi.SysBasicRowGenerator.LANGUAGE_INDEX;
		else if(sysType == SysEnvType.OS.getValue()) 
			fieldIndex = WebSiteSummableKpi.SysBasicRowGenerator.OS_INDEX;

		Map<String, WebSiteSummableKpi> kpiResultMap = summableKpiDao.getWebSiteTopKpi(rowPrefixList, fieldIndex, topNum);
		Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + sysType, visitorType, new ArrayList<String>(kpiResultMap.keySet()));
		Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + sysType, visitorType, new ArrayList<String>(kpiResultMap.keySet()));

		for(String name: kpiResultMap.keySet()){
			WebStats stats = new WebStats();
			fillSummableKpiForWebStats(stats, kpiResultMap.get(name), ipMap.get(name), uvMap.get(name));
			stats.setShowName(name);
			result.put(name, stats); 
		}
		return result;
	}
	
	/**
	 * 赋值：pv, 访问次数，平均访问时间，平均访问页数，跳出率
	 */
	private void fillSummableKpiForWebStats(WebStats stats, WebSiteSummableKpi kpiResult, Long ip, Long uv){
		if(kpiResult == null)
			return;
		stats.setPv(kpiResult.getPv());
		stats.setVisitTimes(kpiResult.getVisitTimes());
		stats.setAvgVisitTime(NumericUtil.getRate(kpiResult.getTotalVisitTime(), kpiResult.getVisitTimes()));
		stats.setAvgVisitPage(NumericUtil.getRate(kpiResult.getPv(), kpiResult.getVisitTimes()));
		stats.setJumpRate(NumericUtil.getRate(kpiResult.getTotalJumpCount(),kpiResult.getVisitTimes()));
		if(ip != null) stats.setIpCount(ip);
		if(uv != null) stats.setUv(uv);
	}
}
