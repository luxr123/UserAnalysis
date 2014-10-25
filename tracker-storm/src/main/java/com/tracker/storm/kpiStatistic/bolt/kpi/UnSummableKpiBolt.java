package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tracker.common.cache.LocalCache;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.service.entity.WebSiteKpiDimension;
import com.tracker.storm.kpiStatistic.service.kpi.SearchKpiService;
import com.tracker.storm.kpiStatistic.service.kpi.WebSiteKpiService;

/**
 * 
 * 文件名：UnSummableKpiBolt
 * 创建人：jason.hua
 * 创建日期：2014-10-22 下午12:02:40
 * 功能描述：不可累加指标
 *
 */
public class UnSummableKpiBolt extends BaseBolt {
	private static final long serialVersionUID = 2840879850884372356L;
	private static Logger LOG = LoggerFactory.getLogger(UnSummableKpiBolt.class);
	private WebSiteKpiService websiteKpiService;
	private SearchKpiService searchKpiService;
	private UnSummableKpiDao unSummableKpiDao;
	private LocalCache<String, String> unSummableKpiKeyCache;
	
	private StormConfig config;//配置对象
	//stream name
	public static String SUMMABLE_KPI_STREAM = "summableKpiStream";
	public static String USER_STATS_STREAM = "userStatsStream";

	public UnSummableKpiBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		websiteKpiService = new WebSiteKpiService(config, true);
		searchKpiService = new SearchKpiService(config);
		unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(config.getHbaseConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_DAY_TABLE);
		unSummableKpiKeyCache = new LocalCache<String, String>(30 * 60);
	}

	@Override
	public void execute(Tuple input) {
		try {
				Boolean isInitSession = input.getBooleanByField(UserSessionBolt.FIELDS.isInitSession.toString());
				Object kpiDimesionObj = input.getValueByField(UserSessionBolt.FIELDS.kpiDimension.toString());
				String cookieId = input.getStringByField(UserSessionBolt.FIELDS.cookieId.toString());
				String userId = input.getStringByField(UserSessionBolt.FIELDS.userId.toString());
				String ip = input.getStringByField(UserSessionBolt.FIELDS.ip.toString());
				Object searchLogObj = input.getValueByField(UserSessionBolt.FIELDS.searchLog.toString());

				if(isInitSession == null || kpiDimesionObj == null || cookieId == null || ip == null){
					LOG.warn(input.getSourceStreamId() + " => has field is null");
					return;
				}
				
				WebSiteKpiDimension kpiDimesion = (WebSiteKpiDimension)kpiDimesionObj;
				List<String> unSummableKpiRowList = new ArrayList<String>();
				if(isInitSession){
					unSummableKpiRowList.addAll(websiteKpiService.computeUnSummableKpiKeyForAll(kpiDimesion, ip, cookieId, userId));
				} else {
					unSummableKpiRowList.addAll(websiteKpiService.computeUnSummableKpiKeyForBasic(kpiDimesion.getDate(), kpiDimesion.getHour(), kpiDimesion.getWebId(), 
							kpiDimesion.getVisitorType(), kpiDimesion.getPageSign(), ip, cookieId, kpiDimesion.getUserType(), userId));
				}
				if(searchLogObj != null){
					unSummableKpiRowList.addAll(searchKpiService.computeUnSummbaleKpiKeys((ApacheSearchLog)searchLogObj));
				}
				unSummableKpiDao.updateUnSummableKpi(cleanUnSummableKpiKeys(unSummableKpiRowList));
				
		} catch(Exception e){
			LOG.error("error to UserSessionBolt, input:" + input, e);
		} 
	}
	
	private List<String> cleanUnSummableKpiKeys(List<String> unSummableKpiRowList){
		List<String> rows = new ArrayList<String>();
		for(String key: unSummableKpiRowList){
			if(unSummableKpiKeyCache.get(key) == null){
				unSummableKpiKeyCache.put(key, key);
				rows.add(key);
			}
		}
		return rows;
	}
}
