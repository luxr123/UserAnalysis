package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.ArrayList;
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

import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.INDEX_FIELDS;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.service.WebSiteSessionService.SessionResult;
import com.tracker.storm.kpiStatistic.service.entity.SummableKpiEntity;
import com.tracker.storm.kpiStatistic.service.entity.UserStatsEntity;
import com.tracker.storm.kpiStatistic.service.entity.WebSiteKpiDimension;
import com.tracker.storm.kpiStatistic.service.kpi.SearchKpiService;
import com.tracker.storm.kpiStatistic.service.kpi.WebSiteKpiService;

/**
 * 
 * 文件名：KpiUpdateBolt
 * 创建人：jason.hua
 * 创建日期：2014-10-15 下午3:50:51
 * 功能描述： 更新可累加kpi值
 *
 */
public class KpiUpdateBolt extends BaseBolt{
	private static final long serialVersionUID = 2840879850884372356L;
	private static Logger LOG = LoggerFactory.getLogger(KpiUpdateBolt.class);
	
	private StormConfig config;//配置对象
	private SummableKpiDao summableKpiDao;//可累加数据访问对象
	private WebSiteKpiService websiteKpiService;
	private SearchKpiService searchKpiService;
	private UnSummableKpiDao unSummableKpiDao;
	private HbaseCRUD cookieIndexTable;
	private HbaseCRUD userIndexTable;
	
	public static String SEARCH_TOP_STREAM = "searchTopStream";

	/**
	 * SummableKpiBolt构造函数
	 */
	public KpiUpdateBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		summableKpiDao = new SummableKpiHBaseDaoImpl(config.getHbaseConnection());
		websiteKpiService = new WebSiteKpiService(config, false);
		searchKpiService = new SearchKpiService(config);
		unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(config.getHbaseConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_DAY_TABLE);
		cookieIndexTable = new HbaseCRUD("cookie_index", config.getZookeeper());
		userIndexTable = new HbaseCRUD("user_index", config.getZookeeper());
	}
 
	@Override
	public void execute(Tuple input) {
		try {
			if(input.getSourceStreamId().equals(UserSessionBolt.SUMMABLE_KPI_STREAM)){
				Object kpiObj = input.getValueByField(UserSessionBolt.FIELDS.kpiObj.toString());
				if(kpiObj == null){
					LOG.warn(input.getSourceStreamId() + " => kpiObj=" + (kpiObj == null?"null":"not null"));
					return;
				}
				updateSummableKpi((SummableKpiEntity)kpiObj);
			} else if(input.getSourceStreamId().equals(UserSessionBolt.KPI_STREAM)){
				Boolean isInitSession = input.getBooleanByField(UserSessionBolt.FIELDS.isInitSession.toString());
				Object kpiDimesionObj = input.getValueByField(UserSessionBolt.FIELDS.kpiDimension.toString());
				String cookieId = input.getStringByField(UserSessionBolt.FIELDS.cookieId.toString());
				String userId = input.getStringByField(UserSessionBolt.FIELDS.userId.toString());
				String ip = input.getStringByField(UserSessionBolt.FIELDS.ip.toString());
				Object searchLogObj = input.getValueByField(UserSessionBolt.FIELDS.searchLog.toString());
				Object sessionResultObj = input.getValueByField(UserSessionBolt.FIELDS.sessionResult.toString());

				if(isInitSession == null || kpiDimesionObj == null || cookieId == null || ip == null || sessionResultObj == null) {
					LOG.warn(input.getSourceStreamId() + " => has field is null");
					return;
				}
				WebSiteKpiDimension kpiDimesion = (WebSiteKpiDimension)kpiDimesionObj;
				SessionResult sessionResult = (SessionResult)sessionResultObj;
				ApacheSearchLog searchLog = null;
				if(searchLogObj != null) 
					searchLog = (ApacheSearchLog)searchLogObj;
				
				//更新不可累加kpi
				updateUnSummableKpi(isInitSession, kpiDimesion, cookieId, userId, ip, searchLog);
				
				//更新可累加kpi
				SummableKpiEntity kpiEntity = websiteKpiService.computeWebSitePVKpi(kpiDimesion);
				kpiEntity.mergeEntity(sessionResult.getKpiEntity());
				if(searchLog != null){
					kpiEntity.mergeEntity(searchKpiService.computeSummableKpi(searchLog));
				}
				updateSummableKpi(kpiEntity);
				
				//更新user统计
				updateUserStats(sessionResult.getUserStatsEntity());
				
				if(searchLog != null && searchLog.isCallSE){
					m_collector.emit(SEARCH_TOP_STREAM, input, new Values(searchLog)); 
				}
			}
		} catch(Exception e){
			LOG.error("error to DataUpdateBolt, input:" + input, e);
		} 
	}
	
	private void updateSummableKpi(SummableKpiEntity kpiEntity){
		if(kpiEntity.getPageKpiMap() != null){
			summableKpiDao.updatePageKpi(kpiEntity.getPageKpiMap());
		}
		if(kpiEntity.getSearchKpiMap() != null){
			summableKpiDao.updateSearchKpi(kpiEntity.getSearchKpiMap());
		} 
		if(kpiEntity.getWebSiteKpiMap() != null){
			summableKpiDao.updateWebSiteKpi(kpiEntity.getWebSiteKpiMap());
		}
	}
	
	private void updateUnSummableKpi(Boolean isInitSession, WebSiteKpiDimension kpiDimesion, String cookieId, String userId, String ip, ApacheSearchLog searchLog){
		List<String> unSummableKpiRowList = new ArrayList<String>();
		if(isInitSession){
			unSummableKpiRowList.addAll(websiteKpiService.computeUnSummableKpiKeyForAll(kpiDimesion, ip, cookieId, userId));
		} else {
			unSummableKpiRowList.addAll(websiteKpiService.computeUnSummableKpiKeyForBasic(kpiDimesion.getDate(), kpiDimesion.getHour(), kpiDimesion.getWebId(), 
					kpiDimesion.getVisitorType(), kpiDimesion.getPageSign(), ip, cookieId, kpiDimesion.getUserType(), userId));
		}
		if(searchLog != null){
			unSummableKpiRowList.addAll(searchKpiService.computeUnSummbaleKpiKeys(searchLog));
		}
		unSummableKpiDao.updateUnSummableKpi(unSummableKpiRowList);
	}
	
	private void updateUserStats(UserStatsEntity statsEntity){
		if(statsEntity == null)
			return;
		
		if(statsEntity.getRowKeyOfCookie() != null){
			HbaseParam param = new HbaseParam();
			param.setRowkey(statsEntity.getRowKeyOfCookie());
			param.addInc(UserVisitLogFields.Index_InfoFam, INDEX_FIELDS.visitTime.toString(), statsEntity.getStayTime());
			cookieIndexTable.batchWrite(param, null);
		}
		if(statsEntity.getRowKeyOfUser() != null){
			HbaseParam param = new HbaseParam();
			param.setRowkey(statsEntity.getRowKeyOfUser());
			param.addInc(UserVisitLogFields.Index_InfoFam, INDEX_FIELDS.visitTime.toString(), statsEntity.getStayTime());
			userIndexTable.batchWrite(param, null);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SEARCH_TOP_STREAM, new Fields(UserSessionBolt.FIELDS.searchLog.toString()));
	}
}
