package com.tracker.api.service.website;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.tracker.api.service.data.FilterDataService;
import com.tracker.api.service.data.WebSiteDataService;
import com.tracker.api.thrift.web.EntryPageStats;
import com.tracker.api.thrift.web.PageStats;
import com.tracker.api.thrift.web.WebStats;
import com.tracker.api.util.StringUtils;
import com.tracker.api.util.TimeUtils;
import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.db.constants.DateType;

public class WebSiteServiceImpl implements WebSiteService{
	private WebSiteService offlineStatsService = new WebSiteOfflineServiceImpl();
	private WebSiteService rtDayStatsService = new WebSiteRTDayServiceImpl();
	private WebSiteDataService webSiteDataService = new WebSiteDataService();
	private FilterDataService filterDataService = new FilterDataService();

	@Override
	public Map<String, WebStats> getWebUserStats(Integer webId,
			Integer timeType, String time) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		Map<String, WebStats> tmpMap = null;
		if(isRealTime && timeType == DateType.DAY.getValue()){
			tmpMap = rtDayStatsService.getWebUserStats(webId, timeType, time);
		} else {
			tmpMap = offlineStatsService.getWebUserStats(webId, timeType, time);
		}
		for(Integer userType: webSiteDataService.getUserType(webId).keySet()){
			WebStats stats = tmpMap.get(userType + "");
			if(stats == null)
				stats = new WebStats();
			String userTypeEnName = webSiteDataService.getUserTypeEnName(webId, userType);
			result.put(userTypeEnName, stats);
		} 
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsByHour(Integer webId,
			Integer timeType, String time, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getWebStatsByHour(webId, timeType, time, visitorType);
		} else {
			result = offlineStatsService.getWebStatsByHour(webId, timeType, time, visitorType);
		}

		for(int hour = 0; hour <= 23; hour++){
			WebStats webStats = result.get(hour + "");
			if(webStats == null){
				webStats = new WebStats();
				result.put(hour + "", webStats);
			}
			webStats.setFieldId(hour);
			webStats.setShowName(hour + "");
			webStats.setTimes(TimeUtils.applyDescForTime(timeType, time));
		}
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsByDates(Integer webId,
			Integer timeType, List<String> times, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, times.get(times.size() - 1));
		if(!isRealTime){
			result.putAll(offlineStatsService.getWebStatsByDates(webId, timeType, times, visitorType));
		} else {
			if(timeType == DateType.DAY.getValue()){
				if(times.size() > 1){
					result.putAll(offlineStatsService.getWebStatsByDates(webId, timeType, times.subList(0, times.size() - 1), visitorType));
				}
				result.putAll(rtDayStatsService.getWebStatsByDates(webId, timeType, Lists.newArrayList(times.get(times.size() - 1)), visitorType));
			} else {
				result.putAll(offlineStatsService.getWebStatsByDates(webId, timeType, times, visitorType));
			}
		}
		
		for(String time: times){
			WebStats webStats = result.get(time);
			if(webStats == null){
				webStats = new WebStats();
				result.put(time, webStats);
			}
			webStats.setShowName(TimeUtils.applyDescForTime(timeType, time));
			webStats.setTimes(TimeUtils.applyDescForTime(timeType, time));
		}
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsForRefType(Integer webId,
			Integer timeType, String time, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getWebStatsForRefType(webId, timeType, time, visitorType);
		} else {
			result = offlineStatsService.getWebStatsForRefType(webId, timeType, time, visitorType);
		}
		for(ReferrerType refTypeObj: ReferrerType.values()){ 
			WebStats webStats = result.get(refTypeObj.getValue()+"");
			if(webStats == null){
				webStats = new WebStats();
				result.put(refTypeObj.getValue() + "", webStats);
			}
			webStats.setShowName(refTypeObj.getName());
			webStats.setTimes(TimeUtils.applyDescForTime(timeType, time));
		}
		return result;
	}

	
	
	@Override
	public Map<String, WebStats> getWebStatsForRefDomain(Integer webId,
			Integer timeType, String time, Integer refType,
			Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getWebStatsForRefDomain(webId, timeType, time, refType, visitorType, topNum);
		} else {
			result = offlineStatsService.getWebStatsForRefDomain(webId, timeType, time, refType, visitorType, topNum);
		}
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsForKeyword(Integer webId,
			Integer timeType, String time, String seDomain,
			Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result =  rtDayStatsService.getWebStatsForKeyword(webId, timeType, time, seDomain, visitorType, topNum);
		} else {
			result =  offlineStatsService.getWebStatsForKeyword(webId, timeType, time, seDomain, visitorType, topNum);
		}
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsForProvince(Integer webId,
			Integer timeType, String time, Integer visitorType,
			Integer countryId) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getWebStatsForProvince(webId, timeType, time, visitorType, countryId);
		} else {
			result = offlineStatsService.getWebStatsForProvince(webId, timeType, time, visitorType, countryId);
		}
		Map<Integer, String> provinceMap = filterDataService.getProvinceMap(countryId);
		for(Integer provinceId: provinceMap.keySet()){
			WebStats webStats = result.get(String.valueOf(provinceId));
			if(webStats == null){
				webStats = new WebStats();
				result.put(String.valueOf(provinceId), webStats);
			}
			String province = provinceMap.get(provinceId);
			webStats.setShowName(StringUtils.removeAreaSuffix(province));
			webStats.setFieldId(provinceId);
			webStats.setTimes(TimeUtils.applyDescForTime(timeType, time));
		}
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsForCity(Integer webId,
			Integer timeType, String time, Integer visitorType,
			Integer countryId, Integer provinceId) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getWebStatsForCity(webId, timeType, time, visitorType, countryId, provinceId);
		} else {
			result = offlineStatsService.getWebStatsForCity(webId, timeType, time, visitorType, countryId, provinceId);
		}
		
		Map<Integer, String> cityMap = filterDataService.getCityMap(countryId, provinceId);
		for(Integer cityId: cityMap.keySet()){ 
			WebStats webStats = result.get(String.valueOf(cityId));
			if(webStats == null){
				webStats = new WebStats();
				result.put(String.valueOf(cityId), webStats);
			}
			String cityName = cityMap.get(cityId) ;
			webStats.setShowName(cityName);
			webStats.setTimes(TimeUtils.applyDescForTime(timeType, time));
		}
		return result;
	}

	@Override
	public Map<String, PageStats> getWebStatsForPage(Integer webId,
			Integer timeType, String time, Integer visitorType) {
		Map<String, PageStats> result = new HashMap<String, PageStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getWebStatsForPage(webId, timeType, time, visitorType);
		} else {
			result = offlineStatsService.getWebStatsForPage(webId, timeType, time, visitorType);
		}
		
		for(String pageSign: result.keySet()){
			PageStats stats = result.get(pageSign);
			stats.setPageDesc(webSiteDataService.getPageDesc(webId, pageSign));
			stats.setPageUrl(webSiteDataService.getWebSiteUrlPrefix(webId) + pageSign);
		}
		return result;
	}

	@Override
	public Map<String, EntryPageStats> getWebStatsForEntryPage(Integer webId,
			Integer timeType, String time, Integer visitorType) {
		Map<String, EntryPageStats> result = new HashMap<String, EntryPageStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result.putAll(rtDayStatsService.getWebStatsForEntryPage(webId, timeType, time, visitorType));		
		} else {
			result.putAll(offlineStatsService.getWebStatsForEntryPage(webId, timeType, time, visitorType));		
		}
		for(String pageSign: result.keySet()){
			EntryPageStats stats = result.get(pageSign);
			stats.setPageDesc(webSiteDataService.getPageDesc(webId, pageSign));
			stats.setPageUrl(webSiteDataService.getWebSiteUrlPrefix(webId) + pageSign);
		}
		return result;
	}

	@Override
	public Map<String, WebStats> getWebStatsForSysEnv(Integer webId,
			Integer timeType, String time, Integer sysType,
			Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result.putAll(rtDayStatsService.getWebStatsForSysEnv(webId, timeType, time, sysType, visitorType, topNum));
		} else {
			result.putAll(offlineStatsService.getWebStatsForSysEnv(webId, timeType, time, sysType, visitorType, topNum));
		}
		for(String key: result.keySet()){
			WebStats stats = result.get(key);
			if(sysType == SysEnvType.COLOR_DEPTH.getValue()){
				stats.setShowName(stats.getShowName() + "-bit");
			} else if(sysType == SysEnvType.COOKIE_ENABLED.getValue()){
				if(stats.getShowName().equalsIgnoreCase("false")){
					stats.setShowName("不支持cookie");
				} else {
					stats.setShowName("支持cookie");
				}
			} 
		}
		return result;
	}
}
