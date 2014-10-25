package com.tracker.api.service.sitesearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.api.Servers;
import com.tracker.api.service.data.SiteSearchDataService;
import com.tracker.api.thrift.search.SearchStats;
import com.tracker.api.thrift.search.SearchStatsResult;
import com.tracker.api.thrift.search.TopResponseTimeResult;
import com.tracker.api.thrift.search.TopResponseTimeStats;
import com.tracker.api.util.NumericUtil;
import com.tracker.api.util.TimeUtils;
import com.tracker.common.constant.search.SearchPageNumType;
import com.tracker.common.constant.search.SearchResultType;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi.ConditionRowGenerator;
import com.tracker.db.dao.kpi.model.SearchSummableKpi.ResultRowGenerator;
import com.tracker.db.dao.siteSearch.SearchRTTopDao;
import com.tracker.db.dao.siteSearch.SearchRTTopHBaseDaoImpl;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult.ResponseTimeRecord;
import com.tracker.db.dao.siteSearch.entity.SearchTopResult;


/**
 * 搜索行为分析本日可累加数据统计
 * 
 * @author jason.hua 
 * 
 */
public class SiteSearchRTDayServiceImpl implements SiteSearchService{
	private SiteSearchDataService searchDataService = new SiteSearchDataService();
	private SummableKpiDao summableKpiDao = new SummableKpiHBaseDaoImpl(Servers.hbaseConnection);
	private UnSummableKpiDao unSummableKpiDayDao = new UnSummableKpiHBaseDaoImpl(Servers.hbaseConnection, UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_DAY_TABLE);
	private SearchRTTopDao searchRTTopDao = new SearchRTTopHBaseDaoImpl(Servers.hbaseConnection);

	/**
	 * 总搜索次数
	 */
	public long getTotalSearchCount(Integer webId, Integer timeType, String time, Integer seId, Integer searchType)  {
		String rowPrefix = ResultRowGenerator.generateRowPrefix(time, String.valueOf(webId), seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType());
		Map<String, SearchSummableKpi> resultMap = summableKpiDao.getSearchKpi(rowPrefix, ResultRowGenerator.DATE_INDEX);
		if(resultMap.containsKey(time)){
			return resultMap.get(time).getPv();
		} else {
			return 0;
		}
	}
	
	/**
	 * 获取基于时间的统计数据
	 */
	public Map<String, SearchStats> getSearchStatsForDates(Integer webId, Integer timeType, List<String> times, Integer seId, Integer searchType) {
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		if(times == null || times.size() == 0)
			return result;
		String time = times.get(0);
		
		//计算pv和总搜索时间
		String rowPrefix = ResultRowGenerator.generateRowPrefix(time, String.valueOf(webId), seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType());
		Map<String, SearchSummableKpi> resultMap = summableKpiDao.getSearchKpi(rowPrefix, ResultRowGenerator.DATE_INDEX);
		SearchSummableKpi kpiResult = resultMap.get(time);
		if(kpiResult == null){
			return result;
		}
		
		//计算首页和翻页次数
		long pageTurningCount = 0;
		long mainPageCount = 0;
		rowPrefix = ResultRowGenerator.generateRowPrefix(time, String.valueOf(webId), seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType());
		Map<String, SearchSummableKpi> pageNumMap = summableKpiDao.getSearchKpi(rowPrefix, ResultRowGenerator.FIELD_VALUE_INDEX);
		for(String key: pageNumMap.keySet()){
			if(key.equals(SearchPageNumType.ONE.getType()+"")){
				mainPageCount = pageNumMap.get(key).getPv();
			} else {
				pageTurningCount += pageNumMap.get(key).getPv();
			}
		}
		
		//计算最大搜索响应时间maxCost
		long maxCost = 0;
		SearchTopResTimeResult recordResult = searchRTTopDao.getMaxRTRecord(time, String.valueOf(webId), seId, searchType, 0, 1);
		List<ResponseTimeRecord> topResTimeList = recordResult.getList();
		if(topResTimeList != null && topResTimeList.size() > 0){
			Integer cost = topResTimeList.get(0).getResponseTime();
			if(cost != null){
				maxCost = cost;
			}
		}
		//不可累加指标， ip， uv
		long ip = unSummableKpiDayDao.getSearchUnSummableKpiForDate(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_SE_DATE, seId, searchType);
		long uv = unSummableKpiDayDao.getSearchUnSummableKpiForDate(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_SE_DATE, seId, searchType);
		
		//赋值
		SearchStats stats = new SearchStats();
		stats.setSearchCount(kpiResult.getPv());
		stats.setPageTurningCount(pageTurningCount);
		stats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(kpiResult.getTotalCost(), kpiResult.getPv()));
		stats.setMainPageCount(mainPageCount);
		stats.setMaxSearchCost(maxCost);
		stats.setIpCount(ip);
		stats.setSearchUserCount(uv);
		result.put(time, stats);
		return result;
	}

	/**
	 * 获取基于展示页码、搜索结果数、响应时间、搜索时段的统计数据
	 */
	public Map<String, SearchStats> getSearchResultStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer resultType){
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		
		String rowPrefix = ResultRowGenerator.generateRowPrefix(time, String.valueOf(webId), seId, searchType, resultType);
		Map<String, SearchSummableKpi> resultMap = summableKpiDao.getSearchKpi(rowPrefix, ResultRowGenerator.FIELD_VALUE_INDEX);
		for(String key: resultMap.keySet()){
			SearchSummableKpi kpiResult = resultMap.get(key);
			SearchStats stats = new SearchStats();
			stats.setSearchCount(kpiResult.getPv());
			stats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(kpiResult.getTotalCost(), kpiResult.getPv()));
			result.put(key, stats);
		}
		return result;
	}
	
	/**
	 * 获取基于搜索条件的统计数据
	 */
	public Map<String, SearchStats> getSearchConditionStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType){
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();

		String rowPrefix = ConditionRowGenerator.generateRowPrefix(time, String.valueOf(webId), seId, searchType);
		Map<String, SearchSummableKpi> resultMap = summableKpiDao.getSearchKpi(rowPrefix, ConditionRowGenerator.CONDITION_TYPE_INDEX);
		for(String key: resultMap.keySet()){
			SearchSummableKpi kpiResult = resultMap.get(key);
			SearchStats stats = new SearchStats();
			stats.setSearchCount(kpiResult.getPv());
			result.put(key, stats);
		}
		return result;
	}

	/**
	 * 基于搜索页面
	 */
	public Map<String, SearchStats> getSearchPageStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType){
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		
		String rowPrefix = ResultRowGenerator.generateRowPrefix(time, String.valueOf(webId), seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType());
		Map<String, SearchSummableKpi> resultMap = summableKpiDao.getSearchKpi(rowPrefix, ResultRowGenerator.SEARCH_PAGE_SHOW_INDEX);
		
		//searchPage + showType
		for(String key: resultMap.keySet()){
			SearchSummableKpi kpiResult = resultMap.get(key);
			SearchStats stats = new SearchStats();
			stats.setSearchCount(kpiResult.getPv());
			result.put(key, stats);
		}
	    return result;
	}
	
	/**
	 * 获取基于搜索值的统计数据
	 */
	public SearchStatsResult getSearchValueStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer conditionType,
			Integer startIndex, Integer offset){
		SearchStatsResult result = new SearchStatsResult(new ArrayList<SearchStats>(), 0);
		SearchTopResult topResult = searchRTTopDao.getMostForSearchValue(time, String.valueOf(webId), seId, searchType, conditionType, startIndex, offset);
		for(SearchTopResult.Entry entry: topResult.getList()){
			SearchStats stats = new SearchStats();
			stats.setName(searchDataService.getSearchValueName(seId, conditionType, entry.getField()));
			stats.setSearchCount(entry.getSearchCount());
			result.addToStatsList(stats);
		}
		result.setTotalCount(topResult.getTotalCount());
		return result;
	}
	
	/**
	 * 获取top最慢响应时间记录
	 */
	public TopResponseTimeResult getTopResponseTimeResult(Integer webId, Integer timeType,String time, Integer seId, Integer searchType,
			Integer startIndex, Integer offset){
		TopResponseTimeResult result = new TopResponseTimeResult(new ArrayList<TopResponseTimeStats>(), 0);
		SearchTopResTimeResult recordResult = searchRTTopDao.getMaxRTRecord(time, String.valueOf(webId), seId, searchType, startIndex, offset);
		for(SearchTopResTimeResult.ResponseTimeRecord record: recordResult.getList()){
			if(record == null)
				continue;
			TopResponseTimeStats stats = new TopResponseTimeStats();
			if(record.getUserId() != null)
				stats.setUserId(Integer.parseInt(record.getUserId()));
			if(record.getUserType() != null)
				stats.setUserType(record.getUserType());
			stats.setCookieId(record.getCookieId());
			stats.setIp(record.getIp());
			if(record.getResponseTime() != null)
				stats.setResponseTime(record.getResponseTime());
			if(record.getSearchTime() != null)
				stats.setVisitTime(TimeUtils.parseTimeToSecond(record.getSearchTime()).split(" ")[1]);
			if(record.getTotalCount() != null)
				stats.setTotalResultCount(record.getTotalCount());
			stats.setSearchValueStr(record.getSearchParam());
			result.addToStatsList(stats);
		}
		result.setTotalCount(recordResult.getTotalCount());
		return result;
	}
	
	/**
	 * 获取top搜索次数最多IP
	 */
	public SearchStatsResult getTopIpResult(Integer webId, Integer timeType,String time, Integer seId, Integer searchType,
			Integer startIndex, Integer offset){
		SearchStatsResult result = new SearchStatsResult(new ArrayList<SearchStats>(), 0);
		SearchTopResult topResult = searchRTTopDao.getMostSearchForIp(time, String.valueOf(webId), seId, searchType, startIndex, offset);
		for(SearchTopResult.Entry entry: topResult.getList()){
			SearchStats stats = new SearchStats();
			stats.setName(entry.getField());
			stats.setSearchCount(entry.getSearchCount());
			result.addToStatsList(stats);
		}
		result.setTotalCount(topResult.getTotalCount());
		return result;
	}
}
