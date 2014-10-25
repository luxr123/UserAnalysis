package com.tracker.api.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.tracker.api.service.data.FilterDataService;
import com.tracker.api.service.website.UserDataService;
import com.tracker.api.service.website.WebSiteService;
import com.tracker.api.service.website.WebSiteServiceImpl;
import com.tracker.api.thrift.web.AreaFilter;
import com.tracker.api.thrift.web.EntryPageStats;
import com.tracker.api.thrift.web.LogFilter;
import com.tracker.api.thrift.web.PageStats;
import com.tracker.api.thrift.web.ReferrerFilter;
import com.tracker.api.thrift.web.UserFilter;
import com.tracker.api.thrift.web.UserInfo;
import com.tracker.api.thrift.web.UserInfoResult;
import com.tracker.api.thrift.web.UserLog;
import com.tracker.api.thrift.web.UserLogResult;
import com.tracker.api.thrift.web.WebStats;
import com.tracker.api.thrift.web.WebStatsService;
import com.tracker.api.util.TimeUtils;

/**
 * 提供网站统计api服务， 主要包括kpi指标、实时访客和实时浏览记录查询
 * 
 * 实现定义的thrift接口{@code WebStatsService}
 * 
 * @author jason.hua
 *
 */
public class WebStatsServiceHandler implements WebStatsService.Iface{
	private Logger logger = LoggerFactory.getLogger(WebStatsServiceHandler.class);
	
	private WebSiteService webSiteStatsService = new WebSiteServiceImpl(); //网站统计服务
	private UserDataService userDataService = new UserDataService(); //用户访问数据服务
	private FilterDataService filterDataService = new FilterDataService(); //过滤数据服务
	private final static int REF_KEYWORD_TOP_NUM = 100;
	private final static int REF_DOMAIN_TOP_NUM = 100;
	private final static int SYS_ENV_TOP_NUM = 25;
	
	/**
	 * totalStats:总的指标, 2：猎头指标, 3：经理人指标, 4：匿名用户指标
	 */
	@Override
	public Map<String, WebStats> getWebSiteStats(int webId, int timeType, String time) throws TException {
		long startTime = System.currentTimeMillis();
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			result.putAll(webSiteStatsService.getWebUserStats(webId, timeType, time));
			Map<String, WebStats> totalStatsMap = webSiteStatsService.getWebStatsByDates(webId, timeType, Lists.newArrayList(time), null);
			if(totalStatsMap != null && totalStatsMap.size() > 0)
				result.put("totalStats", totalStatsMap.get(time));
		} catch(Exception e){
			logger.error("error to getWebSiteStats", e);
		}
		logger.info("getWebSiteStats => " + (System.currentTimeMillis() - startTime));
		return result;
	}
	
	@Override
	public UserInfoResult getUserInfos(int webId, int timeType, String time,
			UserFilter userFilter, int startIndex, int offset)
			throws TException {
		UserInfoResult result = new UserInfoResult(new ArrayList<UserInfo>(), 0);
		try{
			time = TimeUtils.parseTime(timeType, time);
			if(startIndex <= 0)
				startIndex = 0;
			startIndex++;
			result =  userDataService.getUserInfos(webId, startIndex, offset, timeType, time, userFilter);
		} catch(Exception e){
			logger.error("error to getUserInfos => webId:" + webId + ", statIndex:" + startIndex + ", offset:" + offset + ", UserFilter:" + userFilter, e);
		}
		return result;
	}

	@Override
	public UserLogResult getUserLog(int webId, int timeType, String time,
			UserFilter userFilter, LogFilter logFilter, int startIndex, int offset)
			throws TException {
		UserLogResult result = new UserLogResult(new ArrayList<UserLog>(), 0);
		try{
			time = TimeUtils.parseTime(timeType, time);
			if(startIndex < 0)
				startIndex = 0;
			startIndex++;
			
			result = userDataService.getUserLog(webId, startIndex, offset, timeType, time, userFilter, logFilter);
		} catch (Exception e) {
			logger.error("error to get getUserLog => webId:" + webId + ", statIndex:" + startIndex + ", offset:" + offset + ", UserFilter:" + userFilter, e);
		}
		return result;
	}

	@Override
	public List<WebStats> getWebStatsForHour(int webId, int timeType,
			String time, UserFilter userFilter) throws TException {
		List<WebStats> result = new ArrayList<WebStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			Map<String, WebStats> resultMap = webSiteStatsService.getWebStatsByHour(webId, timeType, time, getVisitorType(userFilter));
			result = new ArrayList<WebStats>(resultMap.values());
			Collections.sort(result, new Comparator<WebStats>() {
				@Override
				public int compare(WebStats o1, WebStats o2) {
					return o1.getFieldId() - o2.getFieldId();
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForHour", e);
		}
		return result;
	}

	@Override
	public List<WebStats> getWebStatsForDate(int webId, int timeType,
			List<String> times, UserFilter userFilter) throws TException {
		List<WebStats> result = new ArrayList<WebStats>();
		try{
			if(times.size() < 2)
				return result;
			//离线数据
			times = TimeUtils.parseTimes(timeType, times.get(0), times.get(1));
			if(times.size() > 0) {
				Map<String, WebStats> resultMap = webSiteStatsService.getWebStatsByDates(webId, timeType, times, getVisitorType(userFilter));
				result = new ArrayList<WebStats>(resultMap.values());
				Collections.sort(result, new Comparator<WebStats>() {
					@Override
					public int compare(WebStats o1, WebStats o2) {
						return o1.getShowName().compareTo(o2.getShowName());
					}
				});
			}
		} catch(Exception e){
			logger.error("error to getWebStatsForDate", e);
		}
		return result;
	}

	@Override
	public List<WebStats> getWebStatsForReferrer(int webId, int timeType,
			String time, ReferrerFilter refFilter, UserFilter userFilter,
			int startIndex, int offset) throws TException {
		List<WebStats> result = new ArrayList<WebStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			Map<String, WebStats> resultMap = new HashMap<String, WebStats>();
			if(refFilter == null || refFilter.refType <= 0){
				resultMap = webSiteStatsService.getWebStatsForRefType(webId, timeType, time, getVisitorType(userFilter));
			} else {
				resultMap = webSiteStatsService.getWebStatsForRefDomain(webId, timeType, time, refFilter.refType, getVisitorType(userFilter), REF_DOMAIN_TOP_NUM);
			}
			result = new ArrayList<WebStats>(resultMap.values());
			Collections.sort(result, new Comparator<WebStats>(){
				@Override
				public int compare(WebStats o1, WebStats o2) {
					return (int)(o2.getPv() - o1.getPv());
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForReferrer", e);
		}
		return result;
	}

	@Override
	public List<WebStats> getWebStatsForKeyword(int webId, int timeType,
			String time, int seDomainId, UserFilter userFilter,
			int startIndex, int offset) throws TException {
		List<WebStats> result = new ArrayList<WebStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			String refDomain = null;
			if(seDomainId <= 0){
				refDomain = filterDataService.getSEDomain(seDomainId);
			}
			Map<String, WebStats> resultMap = webSiteStatsService.getWebStatsForKeyword(webId, timeType, time, refDomain, getVisitorType(userFilter), REF_KEYWORD_TOP_NUM);
			result = new ArrayList<WebStats>(resultMap.values());
			Collections.sort(result, new Comparator<WebStats>(){
				@Override
				public int compare(WebStats o1, WebStats o2) {
					return (int)(o2.getPv() - o1.getPv());
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForKeyword", e);
		}
		return result;
	}

	@Override
	public List<WebStats> getWebStatsForArea(int webId, int timeType,
			String time, UserFilter userFilter, AreaFilter areaFilter)
			throws TException {
		List<WebStats> result = new ArrayList<WebStats>();
		try{
			if(areaFilter == null || areaFilter.countryId <= 0)
				return null;
			time = TimeUtils.parseTime(timeType, time);
			Map<String, WebStats> resultMap;
			if(areaFilter.provinceId <= 0)
				resultMap = webSiteStatsService.getWebStatsForProvince(webId, timeType, time, getVisitorType(userFilter), areaFilter.countryId);
			else 
			    resultMap = webSiteStatsService.getWebStatsForCity(webId, timeType, time, getVisitorType(userFilter), areaFilter.countryId, areaFilter.provinceId);
			result = new ArrayList<WebStats>(resultMap.values());
			Collections.sort(result, new Comparator<WebStats>(){
				@Override
				public int compare(WebStats o1, WebStats o2) {
					return (int)(o2.getPv() - o1.getPv());
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForArea", e);
		}
		return result;
	}

	@Override
	public List<PageStats> getWebStatsForPage(int webId, int timeType,
			String time, UserFilter userFilter) throws TException {
		List<PageStats> result = new ArrayList<PageStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			Map<String, PageStats> resultMap = webSiteStatsService.getWebStatsForPage(webId, timeType, time, getVisitorType(userFilter));
			result = new ArrayList<PageStats>(resultMap.values());
			Collections.sort(result, new Comparator<PageStats>(){
				@Override
				public int compare(PageStats o1, PageStats o2) {
					return (int)(o2.getPv() - o1.getPv());
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForPage", e);
		}
		return result;
	}

	@Override
	public List<EntryPageStats> getWebStatsForEntryPage(int webId,
			int timeType, String time, UserFilter userFilter) throws TException {
		List<EntryPageStats> result = new ArrayList<EntryPageStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			Map<String, EntryPageStats> resultMap = webSiteStatsService.getWebStatsForEntryPage(webId, timeType, time, getVisitorType(userFilter));		
			result = new ArrayList<EntryPageStats>(resultMap.values());
			Collections.sort(result, new Comparator<EntryPageStats>(){
				@Override
				public int compare(EntryPageStats o1, EntryPageStats o2) {
					return (int)(o2.getPv() - o1.getPv());
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForEntryPage", e);
		}
		return result;
	}

	@Override
	public List<WebStats> getWebStatsForSysEnv(int webId, int timeType,
			String time, int sysType, UserFilter userFilter) throws TException {
		List<WebStats> result = new ArrayList<WebStats>();
		try{
			time = TimeUtils.parseTime(timeType, time);
			Map<String, WebStats> resultMap = webSiteStatsService.getWebStatsForSysEnv(webId, timeType, time, sysType, getVisitorType(userFilter), SYS_ENV_TOP_NUM);
			result = new ArrayList<WebStats>(resultMap.values());
			Collections.sort(result, new Comparator<WebStats>(){
				@Override
				public int compare(WebStats o1, WebStats o2) {
					return (int)(o2.getPv() - o1.getPv());
				}
			});
		} catch(Exception e){
			logger.error("error to getWebStatsForEntryPage", e);
		}
		return result;
	}
	
	private Integer getVisitorType(UserFilter userFilter){
		if(userFilter == null || userFilter.getVisitorType() == 0){
			return null;
		}
		return userFilter.getVisitorType();
	}
}
