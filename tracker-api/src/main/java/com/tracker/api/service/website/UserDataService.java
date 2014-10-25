package com.tracker.api.service.website;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.tracker.api.Servers;
import com.tracker.api.service.data.WebSiteDataService;
import com.tracker.api.service.website.AccessPaths.PathItem;
import com.tracker.api.service.website.AccessPaths.PathResult;
import com.tracker.api.service.website.UserVisitLog.VisitFields;
import com.tracker.api.service.website.UserVisitLog.VisitResult;
import com.tracker.api.thrift.web.LogFilter;
import com.tracker.api.thrift.web.UserFilter;
import com.tracker.api.thrift.web.UserInfo;
import com.tracker.api.thrift.web.UserInfoResult;
import com.tracker.api.thrift.web.UserLog;
import com.tracker.api.thrift.web.UserLogResult;
import com.tracker.api.thrift.web.WebStats;
import com.tracker.api.util.TimeUtils;
import com.tracker.db.dao.data.model.Page;

/**
 * 实时统计service
 * @author jason.hua
 *
 */
public class UserDataService {
	private static Logger logger = LoggerFactory.getLogger(UserDataService.class);
	
	private UserVisitLog userVisitLog = new UserVisitLog(Servers.prop);
	private AccessPaths accessPaths = new AccessPaths(Servers.ZOOKEEPER);
	private WebSiteDataService webSiteDataService = new WebSiteDataService();
	private WebSiteService statsService = new WebSiteServiceImpl();

	
	/**
	 * 获取用户信息
	 */
	public UserInfoResult getUserInfos(int webId, int startIndex,
			int offset, int timeType, String time, UserFilter userFilter) {
		UserInfoResult result = new UserInfoResult(new ArrayList<UserInfo>(), 0);
		userFilter = getUserFilter(userFilter);
		
		VisitResult listUvlf = null;
		
		String date =  time.substring(0,4) + "-" +  time.substring(4,6) + "-" +  time.substring(6,8);
		if(userFilter.getIp() != null && userFilter.getIp().trim().length() > 0){
			listUvlf = userVisitLog.getLogByIP(String.valueOf(webId), startIndex, offset,  0, date, userFilter);
		} else if(userFilter.getUserId() != null && userFilter.getUserId().trim().length() > 0){
			listUvlf = userVisitLog.getLogByUser(String.valueOf(webId), startIndex, offset,  0, date, userFilter);
		} else if(userFilter.getUserType() > 0){
			if(webSiteDataService.isLoginUser(webId, userFilter.getUserType())){
				listUvlf = userVisitLog.getLogByUser(String.valueOf(webId), startIndex, offset,  0, date, userFilter);
			} else {
				listUvlf = userVisitLog.getLogByCookie(String.valueOf(webId), startIndex, offset,  0, date, userFilter);
			}
		} else {
			listUvlf = userVisitLog.getLogByCookie(String.valueOf(webId), startIndex, offset,  0, date, userFilter);
		}
		 
		if(listUvlf != null){
			for (VisitFields uvlf : listUvlf.getVisits()) {
				UserInfo userInfo = new UserInfo();
				userInfo.setCookieId(uvlf.getCookieId());
				userInfo.setIp(uvlf.getIp());
				if(uvlf.getUserId() != null && uvlf.getUserId().length() > 0)
					userInfo.setUserId(Integer.parseInt(uvlf.getUserId()));
				Integer userType = uvlf.getUserType();
				if(userType != null){
					userInfo.setUserType(userType);
					userInfo.setUserTypeName(webSiteDataService.getUserTypeName(webId, userType));
				}
				userInfo.setLastVisitTime(TimeUtils.parseTimeToSecond(uvlf.getLogTime()).split(" ")[1]);
				userInfo.setUrl(transferCode(uvlf.getCurUrl()));
				Long visitCount = uvlf.getVisitCount();
				if(visitCount != null){
					userInfo.setVisitPages(visitCount.intValue());
				}
				Long totalVisitTime = uvlf.getVisitTime();
				if(totalVisitTime != null){
					userInfo.setTotalVisitTime(totalVisitTime.intValue());
				}
				result.addToUserInfoList(userInfo);
			}
			if(listUvlf.getCount() > 0){
				result.setTotalCount(listUvlf.getCount());
			} else {
				long totalCount = getTotalUserCount(webId, timeType, time, userFilter.getUserType());
				if(result.getUserInfoListSize() > totalCount)
					totalCount = result.getUserInfoListSize();
				result.setTotalCount((int)totalCount);
			}
		}
		return result;
	}
	
	/**
	 * 获取最近用户访问记录
	 */
	public UserLogResult getUserLog(int webId, int startIndex,
			int offset, int timeType, String time, UserFilter userFilter, LogFilter logFilter) {
		UserLogResult result = new UserLogResult(new ArrayList<UserLog>(), 0);
		userFilter = getUserFilter(userFilter);

		PathResult pathResult = null;
		String timeStr =  time.substring(0,4) + "-" +  time.substring(4,6) + "-" +  time.substring(6,8);
		if(userFilter.getIp() != null && userFilter.getIp().trim().length() > 0){
			pathResult = accessPaths.getPathsByIP(String.valueOf(webId), startIndex, offset, logFilter, timeStr, userFilter);
		} else if(userFilter.getUserId() != null && userFilter.getUserId().trim().length() > 0){
			pathResult = accessPaths.getPathsByUser(String.valueOf(webId), startIndex, offset, logFilter, timeStr, userFilter);
		} else {
			pathResult = accessPaths.getPathsByCookie(String.valueOf(webId), startIndex, offset, logFilter, timeStr, userFilter);
		}
		
		if(pathResult != null){
			for(PathItem pitem: pathResult.getList()){
				UserLog userLog = new UserLog(); 
				userLog.setCookieId(pitem.getCookieId());
				userLog.setIp(pitem.getIp());
				if(pitem.getUserId() != null && pitem.getUserId().length() > 0)
					userLog.setUserId(Integer.parseInt(pitem.getUserId()));
				Integer userType = pitem.getUserType();
				if(userType != null){
					userLog.setUserType(userType);
					userLog.setUserTypeName(webSiteDataService.getUserTypeName(webId, userType));
				}
				userLog.setVisitTime(TimeUtils.parseTimeToSecond(pitem.getVisitTime()).split(" ")[1]);
				userLog.setVisitTypeName(webSiteDataService.getPageDesc(webId, Page.getPageSign(pitem.getUrl())));
				userLog.setUrlOrSearchValue(transferCode(pitem.getUrl()));
				if(pitem.getResponseTime() != null)
					userLog.setResponseTime(pitem.getResponseTime());
				if(pitem.getSearchParam() != null)
					userLog.setSearchParam(pitem.getSearchParam());
				if(pitem.getResponseCount() != null)
					userLog.setTotalResultCount(pitem.getResponseCount());
				result.addToUserLogList(userLog);
			}
			if(pathResult.getCount() > 0){
				long count = pathResult.getCount();
				result.setTotalCount((int)count);
			} else {
				long totalCount = getTotalLogCount(webId, timeType, time, userFilter.getUserType());
				if(result.getUserLogListSize() > totalCount)
					totalCount = result.getUserLogListSize();
				result.setTotalCount((int)totalCount);
			}
		}
		return result;
	}
	
	private long getTotalLogCount(int webId, int timeType, String time, int userType){
		long totalPV = 0;
		if(userType > 0){
			Map<String, WebStats> statsMap = statsService.getWebUserStats(webId, timeType, time);
			String userTypeEnName = webSiteDataService.getUserTypeEnName(webId, userType);
			WebStats stats = statsMap.get(userTypeEnName);
			if(stats != null)
				totalPV = stats.getPv();
		} else {
			Map<String, WebStats> statsMap = statsService.getWebStatsByDates(webId, timeType, Lists.newArrayList(time), null);
			WebStats stats = statsMap.get(time);
			if(stats != null)
				totalPV = stats.getPv();
		}
		return totalPV;
	}
	
	private long getTotalUserCount(int webId, int timeType, String time, int userType){
		long totalUv = 0;
		if(userType > 0){
			Map<String, WebStats> statsMap = statsService.getWebUserStats(webId, timeType, time);
			String userTypeEnName = webSiteDataService.getUserTypeEnName(webId, userType);
			WebStats stats = statsMap.get(userTypeEnName);
			if(stats != null)
				totalUv = stats.getUv();
		} else {
			Map<String, WebStats> statsMap = statsService.getWebStatsByDates(webId, timeType, Lists.newArrayList(time), null);
			WebStats stats = statsMap.get(time);
			if(stats != null)
				totalUv = stats.getUv();
		}
		return totalUv;
	}
	
	private UserFilter getUserFilter(UserFilter userFilter){
		UserFilter userFilterTmp = new UserFilter();
		if(userFilter == null)
			return userFilterTmp;
		
		if(userFilter.getIp() != null && userFilter.getIp().trim().length() > 0){
			userFilterTmp.setIp(userFilter.getIp());
		}
		
		if(userFilter.getCookieId() != null && userFilter.getCookieId().trim().length() > 0){
			userFilterTmp.setCookieId(userFilter.getCookieId());
		}
		
		if(userFilter.getUserId() != null && userFilter.getUserId().trim().length() > 0){
			userFilterTmp.setUserId(userFilter.getUserId());
		}
		
		if(userFilter.getUserType() > 0){
			userFilterTmp.setUserType(userFilter.getUserType());
		}
		return userFilterTmp;
	}
	
	private String transferCode(String url){
		try {
			if(url != null){
				url = URLDecoder.decode(url, "gbk");
			}
		} catch (UnsupportedEncodingException e) {
			logger.warn("error to transfer url to gbk, url:" + url);
		}
		return url;
	}
}
