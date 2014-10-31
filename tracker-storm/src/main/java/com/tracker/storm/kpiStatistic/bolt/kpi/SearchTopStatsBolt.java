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

import com.google.common.collect.Lists;
import com.tracker.common.cache.LocalCache;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.utils.DateUtils;
import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.siteSearch.SearchRTTopDao;
import com.tracker.db.dao.siteSearch.SearchRTTopHBaseDaoImpl;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult.ResponseTimeRecord;
import com.tracker.db.dao.siteSearch.entity.SearchValueParam;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.data.DataService;
import com.tracker.storm.kpiStatistic.spout.ApacheLogSpout;

public class SearchTopStatsBolt extends BaseBolt {
	private static final long serialVersionUID = 7784182035554717129L;
	private static Logger LOG = LoggerFactory.getLogger(SearchTopStatsBolt.class);
	private StormConfig config ;
	private DataService dataService = null;
	public static String SEARCH_TOP_VALUE_STREAM = "searchTopValueStream";
	private SearchRTTopDao searchRTTopDao;
	
	private LocalCache<String, ResTimeEntity> topResTimeCache;
	private final static int RES_TIME_TOP_COUNT = 500;

	public SearchTopStatsBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		dataService = new DataService(config.getHbaseConnection());
		searchRTTopDao = new SearchRTTopHBaseDaoImpl(config.getHbaseConnection());
		topResTimeCache = new LocalCache<String, SearchTopStatsBolt.ResTimeEntity>(24 * 60 * 60);
	}

	
	@Override
	public void execute(Tuple input) {
		try{
			Object logObj = input.getValueByField(UserSessionBolt.FIELDS.searchLog.toString());
			if(logObj == null){
				LOG.warn("apacheLogObj is null");
				return;
			}
			ApacheSearchLog log = (ApacheSearchLog)logObj;
			if(!log.getIsCallSE())
				return;
			String webId = log.getWebId();
			Long serverLogTime = log.getServerLogTime();
			String searchEngine = log.getCategory();
			Integer seId = dataService.getSearchEngineId(searchEngine);
			String date = DateUtils.getDay(serverLogTime);
			Integer searchType = log.getSearchType();
			Integer responseTime = log.getResponseTime();
			Long totalCount = log.getTotalCount();
			String searchParam = log.getSearchParam();
			
			/**
			 * 统计Top最慢响应时间
			 */
			if(responseTime != null && searchParam != null && totalCount != null && log.getCookieId() != null){
				if(isTopResTime(date, responseTime)){
					ResponseTimeRecord record = new ResponseTimeRecord(responseTime, serverLogTime, totalCount, searchParam);
					record.setUserId(log.getUserId());
					record.setUserType(log.getUserType());
					record.setIp(log.getIp());
					record.setCookieId(log.getCookieId());
					searchRTTopDao.updateMaxRTRecord(date, webId, seId, searchType, record);
				}
			}
			
			/**
			 * 更新top ip
			 */
			String ip = log.getIp();
			if(ip != null){
				searchRTTopDao.updateMostSearchForIp(date, webId, seId, searchType, ip, 1L);
			}

			/**
			 * 计算并发送Top搜索值
			 */
			String searchConditionJson = log.getSearchConditionJson();
			if(searchConditionJson != null){
				List<SearchValueParam> list = new ArrayList<SearchValueParam>();
				Map<String, Object> seConditionMap = JsonUtil.parseJSON2Map(searchConditionJson);
				//添加搜索次数
				for(String seCondName: seConditionMap.keySet()){
					Integer seConType = dataService.getSearchConditionType(seId, searchType, seCondName);
					list.add(new SearchValueParam(seConType, seConditionMap.get(seCondName).toString(), 1L));
				}
				if(list.size() > 0)
					searchRTTopDao.updateMostForSearchValue(date, webId, seId, searchType, list);
			}	
		} catch(Exception e){
			LOG.error("SearchTopStatsBolt => input:" + input, e);
		}
	}
	
	private boolean isTopResTime(String date, int responseTime){
		ResTimeEntity entity = topResTimeCache.get(date);
		if(entity == null){
			entity = new ResTimeEntity();
			entity.incrTopCount();
			entity.setTopMinResTime(responseTime);
			topResTimeCache.put(date, entity);
			return true;
		} else {
			if(entity.getTopMinResTime() < responseTime || entity.getTopCount() < RES_TIME_TOP_COUNT){
				entity.incrTopCount();
				entity.setTopMinResTime(responseTime);
				topResTimeCache.put(date, entity);
				return true;
			} 
		}
		return false;
	}
	
	private void emitTopValue(Tuple input, String date, String webId, Integer seId, Integer searchType, Integer seConType, String searchValue){
		if(searchValue.trim().length() == 0)
			return;
		m_collector.emit(SEARCH_TOP_VALUE_STREAM, input, new Values(date, webId, seId, searchType, seConType, searchValue));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SEARCH_TOP_VALUE_STREAM, new Fields(FIELDS.date.toString(), FIELDS.webId.toString(), FIELDS.seId.toString(), 
				FIELDS.searchType.toString(), FIELDS.seConType.toString(), FIELDS.searchValue.toString()));
	}

	public static enum FIELDS{
		date, webId, seId, searchType, seConType, searchValue
	}
	
	public static List<String> getInputFields() {
		return Lists.newArrayList(ApacheLogSpout.FIELDS.apacheLogObj.toString(), ApacheLogSpout.FIELDS.ip.toString());
	}
	
	public class ResTimeEntity{
		private long topMinResTime = 0;
		private int topCount = 0;
		
		public long getTopMinResTime() {
			return topMinResTime;
		}
		public void setTopMinResTime(long topMinResTime) {
			this.topMinResTime = topMinResTime;
		}
		public int getTopCount() {
			return topCount;
		}
		public void incrTopCount() {
			this.topCount++;
		}
		
	}
}
