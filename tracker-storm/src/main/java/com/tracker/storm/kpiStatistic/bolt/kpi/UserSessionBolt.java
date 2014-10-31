package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.service.WebSiteSessionService;
import com.tracker.storm.kpiStatistic.service.WebSiteSessionService.SessionResult;
import com.tracker.storm.kpiStatistic.service.entity.SummableKpiEntity;
import com.tracker.storm.kpiStatistic.service.entity.UserStatsEntity;
import com.tracker.storm.kpiStatistic.service.entity.WebSiteKpiDimension;
import com.tracker.storm.kpiStatistic.service.kpi.SearchKpiService;
import com.tracker.storm.kpiStatistic.service.kpi.WebSiteKpiService;
import com.tracker.storm.kpiStatistic.spout.ApacheLogSpout;

/**
 * 
 * 文件名：WebSiteSessionBolt
 * 创建人：jason.hua
 * 创建日期：2014-10-22 下午12:02:40
 * 功能描述：计算会话指标，发送网站统计、站内搜索kpi（可累加、不可累加）到一个节点
 *
 */
public class UserSessionBolt extends BaseBolt {
	private static final long serialVersionUID = 2840879850884372356L;
	private static Logger LOG = LoggerFactory.getLogger(UserSessionBolt.class);
	private WebSiteSessionService sessionService;
	private WebSiteKpiService websiteKpiService;
	private SearchKpiService searchKpiService;

	private StormConfig config;//配置对象
	//stream name
	public static String SUMMABLE_KPI_STREAM = "summableKpiStream";
	public static String USER_STATS_STREAM = "userStatsStream";
	public static String UnSUMMABLE_KPI_STREAM = "unSummableKpiStream";
	public static String KPI_STREAM = "kpiStream";

	public UserSessionBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		sessionService = new WebSiteSessionService(config);
		websiteKpiService = new WebSiteKpiService(config, true);
		searchKpiService = new SearchKpiService(config);
	}

	@Override
	public void execute(Tuple input) {
		try {
			if(input.getSourceStreamId().equals(SessionTimeOutBolt.SESSION_TIME_OUT_STREAM)){
				//清空过期会话
				Long curTime = input.getLongByField(SessionTimeOutBolt.FIELDS.curTime.toString());
				String key = input.getStringByField(SessionTimeOutBolt.FIELDS.key.toString());
				SummableKpiEntity kpiEntity = sessionService.scanAndComputeSession(key, curTime);
				
				//emit可累加会话指标
				emitSummableKpi(input, kpiEntity);
			}  else {
				String logType = input.getStringByField(ApacheLogSpout.FIELDS.logType.toString());
				Object logObj = input.getValueByField(ApacheLogSpout.FIELDS.apacheLogObj.toString());
				if(logObj == null || logType == null){
					LOG.warn(input.getSourceStreamId() + " => apacheLogObj or logType is null");
					return;
				}
				long logTime = 0;
				String cookieId = null;
				String userId = null;
				String ip = null;
				if(ApachePVLog.APACHE_PV_LOG_TYPE.equalsIgnoreCase(logType)){
					ApachePVLog log = (ApachePVLog)logObj;
					cookieId = log.getCookieId();
					userId = log.getUserId();
					ip = log.getIp();
					logTime =  log.getServerLogTime();
				} else if(ApacheSearchLog.APACHE_SEARCH_LOG_TYPE.equalsIgnoreCase(logType)){
					ApacheSearchLog log = (ApacheSearchLog)logObj;
					cookieId = log.getCookieId();
					userId = log.getUserId();
					ip = log.getIp();
					logTime =  log.getServerLogTime();
				}
				WebSiteKpiDimension kpiDimesion = websiteKpiService.getKpiDimension(logType, logObj);

				/**
				 * 实时计算会话指标
				 */
				SessionResult sessionResult = sessionService.computeSession(logTime, cookieId, userId, kpiDimesion);
				
				/**
				 * 计算并发送不可累加kpi
				 */
				if(ApachePVLog.APACHE_PV_LOG_TYPE.equalsIgnoreCase(logType)){
					emitUnSummableKpi(input, sessionResult.isInitSession(), kpiDimesion, cookieId, userId, ip, null);
				} else if(ApacheSearchLog.APACHE_SEARCH_LOG_TYPE.equalsIgnoreCase(logType)){
					emitUnSummableKpi(input, sessionResult.isInitSession(), kpiDimesion, cookieId, userId, ip, logObj);
				}
				
				/**
				 * 计算并发送网站统计和站内搜索kpi
				 */
				SummableKpiEntity kpiEntity = websiteKpiService.computeWebSitePVKpi(kpiDimesion);
				kpiEntity.mergeEntity(sessionResult.getKpiEntity());
				if(ApacheSearchLog.APACHE_SEARCH_LOG_TYPE.equalsIgnoreCase(logType)){
					kpiEntity.mergeEntity(searchKpiService.computeSummableKpi((ApacheSearchLog)logObj));
				}
				emitSummableKpi(input, kpiEntity);
				
				/**
				 * 发送用户统计数据
				 */
				emitUserStats(input, cookieId, sessionResult.getUserStatsEntity());
			}
		} catch(Exception e){
			LOG.error("error to UserSessionBolt, input:" + input, e);
		} 
	}
	
	private void emitUserStats(Tuple input, String cookieId, UserStatsEntity statsEntity){
		if(statsEntity == null ||statsEntity.getStayTime() <= 0)
			return;
		m_collector.emit(USER_STATS_STREAM, input, new Values(cookieId, statsEntity)); 
	}
	
	private void emitSummableKpi(Tuple input, Object obj){
		m_collector.emit(SUMMABLE_KPI_STREAM, input, new Values(obj)); 
	}
	
	private void emitUnSummableKpi(Tuple input, Boolean isInitSession, WebSiteKpiDimension kpiDimension, String cookieId, String userId, String ip, Object searchLog){
		m_collector.emit(UnSUMMABLE_KPI_STREAM, input, new Values(isInitSession, kpiDimension, cookieId, userId, ip, searchLog)); 
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//summable kpi data
		declarer.declareStream(SUMMABLE_KPI_STREAM, new Fields(FIELDS.kpiObj.toString()));
		
		declarer.declareStream(USER_STATS_STREAM, new Fields(FIELDS.cookieId.toString(), FIELDS.statsEntity.toString()));
	
		declarer.declareStream(UnSUMMABLE_KPI_STREAM, new Fields(FIELDS.isInitSession.toString(), FIELDS.kpiDimension.toString()
				, FIELDS.cookieId.toString(), FIELDS.userId.toString(), FIELDS.ip.toString(), FIELDS.searchLog.toString()));
	}
	
	public static enum FIELDS{
		kpiType, kpiSign, kpiObj, statsEntity, kpiDimension, isInitSession, cookieId, userId, ip, searchLog, sessionResult
	}
	
	public static List<String> getInputFields(){
		return Lists.newArrayList(ApacheLogSpout.FIELDS.apacheLogObj.toString(), ApacheLogSpout.FIELDS.logType.toString(), ApacheLogSpout.FIELDS.cookieId.toString());
	}
}
