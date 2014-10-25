package com.tracker.storm.kpiStatistic.service.kpi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.common.constant.search.SearchCostType;
import com.tracker.common.constant.search.SearchPageNumType;
import com.tracker.common.constant.search.SearchResultCountType;
import com.tracker.common.constant.search.SearchResultType;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.utils.DateUtils;
import com.tracker.common.utils.JsonUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam.SearchRowGenerator;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi.ConditionRowGenerator;
import com.tracker.db.dao.kpi.model.SearchSummableKpi.ResultRowGenerator;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.data.DataService;
import com.tracker.storm.kpiStatistic.service.entity.SummableKpiEntity;

public class SearchKpiService {
	private DataService dataService = null;
	
	public SearchKpiService(StormConfig config){
		dataService = new DataService(config.getHbaseConnection());
	}
	
	/**
	 * 
	 * 函数名：computeSearchKpi
	 * 功能描述：计算并返回站内搜索可累加kpi值
	 * @param log
	 * @return
	 */
	public SummableKpiEntity computeSummableKpi(ApacheSearchLog log){
		String searchEngine = log.getCategory();
		Long serverLogTime = log.getServerLogTime();
		Boolean isCallSE = log.getIsCallSE();
		String webId = log.getWebId();
		
		String date = DateUtils.getDay(serverLogTime);
		Integer searchType = log.getSearchType();
		Integer seId = dataService.getSearchEngineId(searchEngine);
		Integer curPageNum = log.getCurPageNum();
		
		String searchPageShowType = StringUtil.getPageName(log.getCurUrl()) + "-" + log.getSearchShowType();
		//---------------------------- summable kpi------------------------------------
		Map<String, SearchSummableKpi> kpiMap = new HashMap<String, SearchSummableKpi>();
		if(isCallSE){
			int hour = DateUtils.getTime(serverLogTime);
			Integer responseTime = log.getResponseTime();
			Long totalCount = log.getTotalCount();
			
			//基于搜索结果kpi
			setKpiForResult(kpiMap, date, webId, seId, searchType, searchPageShowType, responseTime, totalCount, hour);
			
			//基于搜索条件kpi
			setKpiForSearchCondition(kpiMap, date, webId, seId, searchType, log.getSearchShowType(), log.getSearchConditionJson());
		}
		//for 页码
		setKpiForPageNum(kpiMap, date, webId, seId, searchType, searchPageShowType, curPageNum);
		
		SummableKpiEntity kpiEntity = new SummableKpiEntity();
		for(String key: kpiMap.keySet()){
			kpiEntity.addSearchKpi(key, kpiMap.get(key));
		}
		return kpiEntity;
	}
	
	public List<String> computeUnSummbaleKpiKeys(ApacheSearchLog searchLog){
		List<String> rows = new ArrayList<String>();
		String ip = searchLog.getIp();
		String cookieId = searchLog.getCookieId();
		Integer seId = dataService.getSearchEngineId(searchLog.getCategory());
		String date = DateUtils.getDay(searchLog.getServerLogTime());
		String webId = searchLog.getWebId();
		Integer searchType = searchLog.getSearchType();
		
		//更新ip数
		if(ip != null){
			String row = SearchRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_SE_DATE, UnSummableKpiParam.KPI_IP, date, webId, seId, searchType, ip);
			rows.add(row);
		}

		//更新搜索人数
		if(cookieId != null){
			String row = SearchRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_SE_DATE, UnSummableKpiParam.KPI_UV, date, webId, seId, searchType, cookieId);
			rows.add(row);
		}
		return rows;
	}
	
	//for result
	private void setKpiForResult(Map<String, SearchSummableKpi> kpiMap, String date, String webId, Integer seId, Integer searchType,
			String searchPageShowType, Integer responseTime, Long totalCount, int hour){
		SearchCostType costType = responseTime == null? null:SearchCostType.getType(responseTime);
		SearchResultCountType resultCountType = totalCount == null ? null: SearchResultCountType.getType(totalCount);
		SearchSummableKpi pvAndCostKpi = new SearchSummableKpi();
		pvAndCostKpi.setPv(1L);
		if(responseTime != null)
			pvAndCostKpi.setTotalCost((long)responseTime);
		
		//for result:search cost
		if(costType != null){
			String resultCostRow = ResultRowGenerator.generateRowKey(date, webId, seId, searchType, SearchResultType.SEARCH_COST.getType(), searchPageShowType, costType.getType());
			kpiMap.put(resultCostRow, pvAndCostKpi);
		}
		// for result: count
		if(resultCountType != null){
			String resultCountRow = ResultRowGenerator.generateRowKey(date, webId, seId, searchType, SearchResultType.SEARCH_RESULT_COUNT.getType(), searchPageShowType, resultCountType.getType());
			kpiMap.put(resultCountRow, pvAndCostKpi);
		}
		// for result: time
		String resultTimeRow = ResultRowGenerator.generateRowKey(date, webId, seId, searchType, SearchResultType.SEARCH_TIME.getType(), searchPageShowType, hour);
		kpiMap.put(resultTimeRow, pvAndCostKpi);
	}
	
	//for search condition
	private void setKpiForSearchCondition(Map<String, SearchSummableKpi> kpiMap, String date, String webId, Integer seId, Integer searchType, Integer searchShowType, String searchConditionJson){
		if(searchConditionJson == null)
			return;
		SearchSummableKpi pvKpi = new SearchSummableKpi();
		pvKpi.setPv(1L);
		Map<String, Object> seConditionMap = JsonUtil.parseJSON2Map(searchConditionJson);
		
		//过滤掉默认值
		if(seId != null && searchShowType != null){
			Map<String, String> defaultValueMap = dataService.getSearchDefaultValue(seId, searchShowType);
			for(String field: defaultValueMap.keySet()){
				Object value = seConditionMap.get(field);
				if(value != null && value.toString().equals(defaultValueMap.get(field))){
					seConditionMap.remove(field);
				}
			}
		}

		//添加搜索次数
		for(String fieldName: seConditionMap.keySet()){
			Integer seConType = dataService.getSearchConditionType(seId, searchType, fieldName);
			String conFieldRow = ConditionRowGenerator.generateRowKey(date, webId, seId, searchType, seConType);
			kpiMap.put(conFieldRow, pvKpi);
		}
	}
	
	//for pageNum
	private void setKpiForPageNum(Map<String, SearchSummableKpi> kpiMap, String date, String webId, Integer seId, Integer searchType, String searchPageShowType, Integer curPageNum){
		if(curPageNum == null)
			return;
		SearchPageNumType pageNumType = SearchPageNumType.getType(curPageNum);
		if(pageNumType != null){
			SearchSummableKpi pvKpi = new SearchSummableKpi();
			pvKpi.setPv(1L);
			String pageNumRow = ResultRowGenerator.generateRowKey(date, webId, seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType(), searchPageShowType, pageNumType.getType());
			kpiMap.put(pageNumRow, pvKpi);
		}
	}
}
