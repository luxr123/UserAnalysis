package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;
import com.tracker.db.dao.siteSearch.SearchRTTopDao;
import com.tracker.db.dao.siteSearch.SearchRTTopHBaseDaoImpl;
import com.tracker.db.dao.siteSearch.entity.SearchValueParam;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;

public class SearchTopValueBolt extends BaseBolt {
	private static final long serialVersionUID = 7784182035554717129L;
	private static Logger LOG = LoggerFactory.getLogger(SearchTopValueBolt.class);
	private StormConfig config ;
	public static String SEARCH_TOP_VALUE_STREAM = "searchTopValueStream";
	private SearchRTTopDao searchRTTopDao;

	
	public SearchTopValueBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		searchRTTopDao = new SearchRTTopHBaseDaoImpl(config.getHbaseConnection());
	}

	
	@Override
	public void execute(Tuple input) {
		try{
			String date = input.getStringByField(SearchTopStatsBolt.FIELDS.date.toString());
			String webId = input.getStringByField(SearchTopStatsBolt.FIELDS.webId.toString());
			Integer seId = input.getIntegerByField(SearchTopStatsBolt.FIELDS.seId.toString());
			Integer seConType = input.getIntegerByField(SearchTopStatsBolt.FIELDS.seConType.toString());
			Integer searchType = input.getIntegerByField(SearchTopStatsBolt.FIELDS.searchType.toString());
			String searchValue = input.getStringByField(SearchTopStatsBolt.FIELDS.searchValue.toString());
			
			if(date == null || webId == null || seId == null || seConType == null || searchValue == null){
				LOG.warn("some field of input data is null");
				return;
			}
			searchRTTopDao.updateMostForSearchValue(date, webId, seId, searchType, Lists.newArrayList(new SearchValueParam(seConType, searchValue, 1L)));
		} catch(Exception e){
			LOG.error("SearchTopStatsBolt => input:" + input, e);
		}
	}
}
