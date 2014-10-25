package com.tracker.storm.kpiStatistic.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.utils.JsonUtil;
import com.tracker.common.utils.TableRowKeyCompUtil;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.db.dao.webstats.UserSessionDao;
import com.tracker.db.dao.webstats.UserSessionHBaseDaoImpl;
import com.tracker.db.dao.webstats.model.UserSessionData;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.kpiStatistic.service.entity.SummableKpiEntity;
import com.tracker.storm.kpiStatistic.service.entity.UserStatsEntity;
import com.tracker.storm.kpiStatistic.service.entity.WebSiteKpiDimension;
import com.tracker.storm.kpiStatistic.service.kpi.WebSiteKpiRowGenerator;

/**
 * 实时会话计算
 * @author jason.hua
 *
 */
/**
 * 文件名：WebSiteSessionService
 * 创建人：jason.hua
 * 创建日期：2014-10-11 下午3:33:15
 * 功能描述：
 *  
 */
public class WebSiteSessionService {
	private static Logger LOG = LoggerFactory.getLogger(WebSiteSessionService.class);
	private UserSessionDao userSessionDao;
	private final int sessionGapTime = 30 * 60 * 1000; // 30分钟

	/**
	 * 构造方法的描述.
	 * 创建人：jason.hua
	 * 创建日期：2014-10-11 下午3:53:32
	 * @param props
	 */
	public WebSiteSessionService(StormConfig config){
		userSessionDao = new UserSessionHBaseDaoImpl(config.getHbaseConnection());
	}
	
	/**
	 * 
	 * 函数名：computeSession
	 * 功能描述：计算用户会话
	 * @param date
	 * @param webId
	 * @param pageSign
	 * @param logTime
	 * @param cookieId
	 * @param userId
	 * @param kpiDimesion
	 * @return
	 */
	public SessionResult computeSession(long logTime, String cookieId, String userId, WebSiteKpiDimension kpiDimesion){
		SessionResult result = new SessionResult();
		
		int refType = kpiDimesion.getRefType();
		String date = kpiDimesion.getDate();
		String webId = kpiDimesion.getWebId();
		String pageSign = kpiDimesion.getPageSign();
		String sessionRowKey = UserSessionData.generateKey(date, webId, cookieId);
		//来源是外部链接, 则会话结束
		if(refType == ReferrerType.SEARCH_ENGINE.getValue() || refType == ReferrerType.OTHER_LINK.getValue()){
			SummableKpiEntity kpiEntity = endUserSession(sessionRowKey);
			kpiEntity.mergeEntity(initUserSession(sessionRowKey, logTime, kpiDimesion));
			result.setKpiEntity(kpiEntity);
			result.setInitSession(true);
			return result;
		} 
		
		//获取用户上次会话记录
		UserSessionData sessionData = userSessionDao.getUserSession(sessionRowKey);
		//如果用户会话不存在，或者会话缺失，重新初始化会话
		if(sessionData == null || sessionData.getLastVisitTime() == null || sessionData.getLastPageSign() == null){
			SummableKpiEntity kpiEntity = initUserSession(sessionRowKey, logTime, kpiDimesion);
			result.setKpiEntity(kpiEntity);
			result.setInitSession(true);
			return result;
		} 
		 //更新用户上次访问时间
		Long lastVisitTime = sessionData.getLastVisitTime();
		if(lastVisitTime > logTime){
			LOG.warn("session visitTime is error, date:" + date + ", webId:" + webId + ", cookieId:" + cookieId + " => lastVisitTime:" + lastVisitTime + ", thisVisitTime:" + logTime);
			result.setInitSession(false);
			return result;
		}
		
		//如果上次访问时间在30分钟外，则会话结束
		long gap = logTime - lastVisitTime;
		if(gap > sessionGapTime){
			SummableKpiEntity kpiEntity = endUserSession(sessionRowKey, sessionData);
			kpiEntity.mergeEntity(initUserSession(sessionRowKey, logTime, kpiDimesion));
			result.setKpiEntity(kpiEntity);
			result.setInitSession(true);
			return result;
		}

		//更新用户会话（访问时间、访问页面）
		userSessionDao.updateUserSession(sessionRowKey, logTime, pageSign);

		//计算会话指标
		long stayTime = gap / 1000;
		if(stayTime > 0){
			SummableKpiEntity kpiEntity = computeUserSessionKpi(date, sessionData, stayTime);
			//保存各个用户的访问时长
			result.setUserStatsEntity(getUserStatsEntity(date, webId, userId, kpiDimesion.getUserType(), cookieId, stayTime));
			result.setKpiEntity(kpiEntity);
		}
		result.setInitSession(false);
		return result;
	}
	
	/**
	 * 扫描、计算过期会话，并删除
	 */
	public SummableKpiEntity scanAndComputeSession(String key,  long curTime){
		// 每分钟遍历并计算结束的会话（针对直接访问，且上次访问时间在30分钟外）
		UserSessionData userSessionData = userSessionDao.getUserSession(key);
		if(userSessionData != null && userSessionData.getLastVisitTime() != null){
			if(userSessionData.getLastVisitTime() <= curTime){
				return endUserSession(key, userSessionData);
			}
		}
		return new SummableKpiEntity();
	}
	
	/**
	 * 初始化用户会话
	 * 
	 * 会话指标：访问次数
	 * 入口页：pv数、访问次数、访问页数
	 * 受访页：入口页次数
	 */
	private SummableKpiEntity initUserSession(String sessionRowKey, long logTime, WebSiteKpiDimension kpiDimesion){
		String date = kpiDimesion.getDate();
		String webId = kpiDimesion.getWebId();
		String pageSign = kpiDimesion.getPageSign();
		
		UserSessionData data = new UserSessionData();
		data.setLastVisitTime(logTime);
		data.setLastPageSign(pageSign);
		data.setStartVisitTime(logTime);
		data.setKpiDimesion(JsonUtil.toJson(kpiDimesion));
		
		userSessionDao.updateUserSession(sessionRowKey, data);

		SummableKpiEntity kpiEntity = new SummableKpiEntity();
		//更新访问次数
		WebSiteSummableKpi kpi = new WebSiteSummableKpi();
		kpi.setVisitTimes(1L);
		List<String> webSiteKpiRows = WebSiteKpiRowGenerator.getWebSiteSummableKpiRows(kpiDimesion);
		for(String rowKey: webSiteKpiRows){
			kpiEntity.addWebSiteKpi(rowKey, kpi);
		}
		
		if(pageSign != null){
			//入口页:pv数、访问次数、访问页数
			WebSiteSummableKpi entryPageKpi = new WebSiteSummableKpi();
			entryPageKpi.setPv(1L);
			entryPageKpi.setVisitTimes(1L);
			entryPageKpi.setTotalVisitPage(1L);
			String entryPageRow = WebSiteKpiRowGenerator.getEntryPageSummableKpiRow(date, webId, pageSign, kpiDimesion.getVisitorType());
			kpiEntity.addWebSiteKpi(entryPageRow, entryPageKpi);
		
			//受访页：入口页次数
			PageSummableKpi pageKpi = new PageSummableKpi();
			pageKpi.setEntryPageCount(1L);
			String pageRow = WebSiteKpiRowGenerator.getPageSummableKpiRow(date, webId, pageSign, kpiDimesion.getVisitorType());
			kpiEntity.addPageKpi(pageRow, pageKpi);
		}
		return kpiEntity;
	}
	
	/**
	 * 结束用户会话,并删除
	 * 
	 * 会话指标：跳出次数
	 * 受访页：退出次数
	 */
	private SummableKpiEntity endUserSession(String ...rowKeys){
		Map<String, UserSessionData> sessionDataMap  = userSessionDao.getUserSessions(rowKeys);
		SummableKpiEntity kpiEntity = endUserSession(sessionDataMap);
		//delete user session
		userSessionDao.deleteUserSession(rowKeys);
		return kpiEntity;
	}
	
	private SummableKpiEntity endUserSession(String key, UserSessionData sessionData){
		Map<String, UserSessionData> sessionDataMap = new HashMap<String, UserSessionData>();
		sessionDataMap.put(key, sessionData);
		SummableKpiEntity kpiEntity = endUserSession(sessionDataMap);
		userSessionDao.deleteUserSession(key);
		return kpiEntity;
	}
	
	private SummableKpiEntity endUserSession(Map<String, UserSessionData> sessionDataMap){
		Map<String, Long> jumpMap = new HashMap<String, Long>();
		Map<String, Long> outPageMap = new HashMap<String, Long>();

		for(String key: sessionDataMap.keySet()){
			UserSessionData data = sessionDataMap.get(key);
			WebSiteKpiDimension kpiDimesion = JsonUtil.toObject(data.getKpiDimesion(), WebSiteKpiDimension.class);
			String date = kpiDimesion.getDate();
			String webId = kpiDimesion.getWebId();
			String entryPageSign = kpiDimesion.getPageSign();
			String lastPageSign = data.getLastPageSign();
			Long startVisitTime = data.getStartVisitTime();
			Long lastVisitTime = data.getLastVisitTime();
			
			//更新跳出次数
			if(lastVisitTime != null && startVisitTime != null && lastVisitTime == startVisitTime ){
				List<String> webSiteKpiRows = WebSiteKpiRowGenerator.getWebSiteSummableKpiRows(kpiDimesion);
				if(entryPageSign != null){
					String entryPageRow = WebSiteKpiRowGenerator.getEntryPageSummableKpiRow(date, webId, entryPageSign, kpiDimesion.getVisitorType());
					webSiteKpiRows.add(entryPageRow);
				}
				for(String row: webSiteKpiRows){
					if(jumpMap.containsKey(row)){
						jumpMap.put(row, jumpMap.get(row) + 1);
					} else{
						jumpMap.put(row, 1L);
					}
				}
			}
			//受访页 - 退出次数
			if(lastPageSign != null){
				String kpiRow = WebSiteKpiRowGenerator.getPageSummableKpiRow(date, webId, lastPageSign, kpiDimesion.getVisitorType());
				if(outPageMap.containsKey(kpiRow)){
					outPageMap.put(kpiRow, outPageMap.get(kpiRow) + 1);
				} else{
					outPageMap.put(kpiRow, 1L);
				}			
			}
		}
		SummableKpiEntity kpiEntity = new SummableKpiEntity();
		//更新kpi
		if(jumpMap.size() > 0){
			for(String key: jumpMap.keySet()){
				WebSiteSummableKpi kpi = new WebSiteSummableKpi();
				kpi.setTotalJumpCount(jumpMap.get(key));
				kpiEntity.addWebSiteKpi(key, kpi);
			}
		}
		if(outPageMap.size() > 0){
			for(String key: outPageMap.keySet()){
				PageSummableKpi kpi = new PageSummableKpi();
				kpi.setOutPageCount(outPageMap.get(key));
				kpiEntity.addPageKpi(key, kpi);
			}
		}
		return kpiEntity;
	}
	
	/**
	 * 计算会话
	 * 
	 * 会话指标：访问时长
	 * 受访页：贡献下游次数、停留时间
	 * 入口页：访问时长、访问页数
	 * @param stayTime 秒
	 */
	private SummableKpiEntity computeUserSessionKpi(String date, UserSessionData data, long stayTime){
		WebSiteKpiDimension kpiDimesion = JsonUtil.toObject(data.getKpiDimesion(), WebSiteKpiDimension.class);;
		String entryPageSign = kpiDimesion.getPageSign();
		String webId = kpiDimesion.getWebId();
		String lastPageSign = data.getLastPageSign();
		
		SummableKpiEntity kpiEntity = new SummableKpiEntity();

		//受访页：贡献下游次数、停留时间
		PageSummableKpi pageKpi = new PageSummableKpi();
		pageKpi.setStayTime(stayTime);
		pageKpi.setNextPageCount(1L);
		String pageRow = WebSiteKpiRowGenerator.getPageSummableKpiRow(date, webId, lastPageSign, kpiDimesion.getVisitorType());
		kpiEntity.addPageKpi(pageRow, pageKpi);
		
		//访问时长
		WebSiteSummableKpi webSitePageKpi = new WebSiteSummableKpi();
		webSitePageKpi.setTotalVisitTime(stayTime);
		List<String> webSiteKpiRows = WebSiteKpiRowGenerator.getWebSiteSummableKpiRows(kpiDimesion);
		for(String key: webSiteKpiRows){
			kpiEntity.addWebSiteKpi(key, webSitePageKpi);
		}

		//入口页：访问时长、访问页数
		if(entryPageSign != null){
			String entryPageRow = WebSiteKpiRowGenerator.getEntryPageSummableKpiRow(date, webId, entryPageSign, kpiDimesion.getVisitorType());
			WebSiteSummableKpi entryPageKpi = new WebSiteSummableKpi();
			entryPageKpi.setTotalVisitTime(stayTime);
			entryPageKpi.setTotalVisitPage(1L);
			kpiEntity.addWebSiteKpi(entryPageRow, entryPageKpi);
		}
		return kpiEntity;
	}
	
	/**
	 * 保存各个cookie和userId的访问时长
	 */
	private UserStatsEntity getUserStatsEntity(String date, String webId, String userId, Integer userType, String cookieId, long stayTime){
		UserStatsEntity entity = new UserStatsEntity();
		entity.setStayTime(stayTime);
		if (userId != null && userType != null) {
			entity.setRowKeyOfUser(TableRowKeyCompUtil.getPartitionRowKey(userId, webId, date, Lists.newArrayList(userType + "")));
		}
		//保存cookie的访问时长
		if(cookieId != null && userType != null){
			entity.setRowKeyOfCookie(TableRowKeyCompUtil.getPartitionRowKey(cookieId, webId, date, Lists.newArrayList(userType + "")));
		}
		return entity;
	}
	
	public static class SessionResult{
		private boolean isInitSession = false;
		private SummableKpiEntity kpiEntity;
		private UserStatsEntity userStatsEntity;
		
		public boolean isInitSession() {
			return isInitSession;
		}
		public void setInitSession(boolean isInitSession) {
			this.isInitSession = isInitSession;
		}
		public SummableKpiEntity getKpiEntity() {
			return kpiEntity;
		}
		public void setKpiEntity(SummableKpiEntity kpiEntity) {
			this.kpiEntity = kpiEntity;
		}
		public UserStatsEntity getUserStatsEntity() {
			return userStatsEntity;
		}
		public void setUserStatsEntity(UserStatsEntity userStatsEntity) {
			this.userStatsEntity = userStatsEntity;
		}
	}
}
