package com.tracker.api.service.sitesearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.tracker.api.service.data.SiteSearchDataService;
import com.tracker.api.service.data.WebSiteDataService;
import com.tracker.api.thrift.search.SearchStats;
import com.tracker.api.thrift.search.SearchStatsResult;
import com.tracker.api.thrift.search.TopResponseTimeResult;
import com.tracker.api.util.NumericUtil;
import com.tracker.api.util.TimeUtils;
import com.tracker.common.constant.search.SearchCostType;
import com.tracker.common.constant.search.SearchPageNumType;
import com.tracker.common.constant.search.SearchResultCountType;
import com.tracker.common.constant.search.SearchResultType;
import com.tracker.db.constants.DateType;


/**
 * 数据服务接口（日、周、月、年）
 * 
 * @author jason.hua
 * 
 */
public class SiteSearchServiceImpl implements SiteSearchService{
	private SiteSearchService offlineStatsService = new SiteSearchOfflineServiceImpl();
	private SiteSearchService rtDayStatsService = new SiteSearchRTDayServiceImpl();

	private SiteSearchDataService searchDataService = new SiteSearchDataService();

	@Override
	public long getTotalSearchCount(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType) {
		long totalCount = 0;
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			totalCount = rtDayStatsService.getTotalSearchCount(webId, timeType, time, seId, searchType);
		} else {
			totalCount = offlineStatsService.getTotalSearchCount(webId, timeType, time, seId, searchType);
		}
		return totalCount;
	}

	@Override
	public Map<String, SearchStats> getSearchStatsForDates(Integer webId,
			Integer timeType, List<String> times, Integer seId,
			Integer searchType) {
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, times.get(times.size() - 1));
		if(!isRealTime){
			result = offlineStatsService.getSearchStatsForDates(webId, timeType, times, seId, searchType);
		} else {
			if(timeType == DateType.DAY.getValue()){
				if(times.size() > 1)
					result.putAll(offlineStatsService.getSearchStatsForDates(webId, timeType, times.subList(0, times.size() - 1), seId, searchType));
				result.putAll(rtDayStatsService.getSearchStatsForDates(webId, timeType, Lists.newArrayList(times.get(times.size() - 1)), seId, searchType));
			} else {
				result.putAll(offlineStatsService.getSearchStatsForDates(webId, timeType, times, seId, searchType));
			}
			
		}
		for(String time : times){
			SearchStats stats = result.get(time);
			if(stats == null){
				stats = new SearchStats();
				result.put(time, stats);
			}
			stats.setName(TimeUtils.applyDescForTime(timeType, time));
			stats.setDate(TimeUtils.applyDescForTime(timeType, time));
		}
		return result;
	}

	@Override
	public Map<String, SearchStats> getSearchResultStats(Integer webId,
			Integer timeType, String time, Integer seId, Integer searchType,
			Integer resultType) {
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getSearchResultStats(webId, timeType, time, seId, searchType, resultType);
		} else {
			result = offlineStatsService.getSearchResultStats(webId, timeType, time, seId, searchType, resultType);
		}
		long totalCount = getTotalSearchCount(webId, timeType, time, seId, searchType);
		//supply
		for(int resultTypeValue: SearchResultType.getResultTypeValues(resultType)){
			SearchStats stats = result.get(String.valueOf(resultTypeValue));
			if(stats == null){
				stats = new SearchStats();
				result.put(String.valueOf(resultTypeValue), stats);
			}
			String name = null;
			if(resultType == SearchResultType.DISPLAY_PAGE_NUM.getType()){
				SearchPageNumType type = SearchPageNumType.valueOf(resultTypeValue);
				name = (type != null ? type.getDesc(): "error value:" + resultTypeValue);
			} else if(resultType == SearchResultType.SEARCH_RESULT_COUNT.getType()){
				SearchResultCountType type = SearchResultCountType.valueOf(resultTypeValue);
				name = (type != null ? type.getDesc(): "error value:" + resultTypeValue);
			} else if(resultType == SearchResultType.SEARCH_COST.getType()){
				SearchCostType type = SearchCostType.valueOf(resultTypeValue);
				name = (type != null ? type.getDesc(): "error value:" + resultTypeValue);
			} else if(resultType == SearchResultType.SEARCH_TIME.getType()){
				name = resultTypeValue + "";
			}
			stats.setName(name);
			stats.setFieldId(resultTypeValue);
			stats.setDate(TimeUtils.applyDescForTime(timeType, time));
			stats.setSearchCountRate(NumericUtil.getRateForPCT(stats.getSearchCount(), totalCount));
		}
		return result;
	}

	@Override
	public Map<String, SearchStats> getSearchConditionStats(Integer webId,
			Integer timeType, String time, Integer seId, Integer searchType) {
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		Map<String, SearchStats> resultMap = new HashMap<String, SearchStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			resultMap = rtDayStatsService.getSearchConditionStats(webId, timeType, time, seId, searchType);
		} else {
			resultMap = offlineStatsService.getSearchConditionStats(webId, timeType, time, seId, searchType);
		}
		long totalCount = getTotalSearchCount(webId, timeType, time, seId, searchType);
		Map<Integer, String> conTypeMap = searchDataService.getSearchCondition(seId, searchType);
		for(Integer conType: conTypeMap.keySet()){
			SearchStats stats = resultMap.get(String.valueOf(conType));
			if(stats == null){
				stats = new SearchStats();
			}
			stats.setName(conTypeMap.get(conType));
			stats.setSearchCountRate(NumericUtil.getRateForPCT(stats.getSearchCount(), totalCount));
			stats.setDate(TimeUtils.applyDescForTime(timeType, time));
			stats.setFieldId(conType);
			result.put(String.valueOf(conType), stats);
		}
		return result;
	}

	@Override
	public Map<String, SearchStats> getSearchPageStats(Integer webId,
			Integer timeType, String time, Integer seId, Integer searchType) {
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getSearchPageStats(webId, timeType, time, seId, searchType);
		} else {
			result = offlineStatsService.getSearchPageStats(webId, timeType, time, seId, searchType);
		}
		
		Map<String, String>  searchPageMap = searchDataService.getSearchShowTypeMap(webId, seId, searchType);
	    long totalCount = getTotalSearchCount(webId, timeType, time, seId, searchType);
        for (String searchPageShowType: searchPageMap.keySet()){
			SearchStats stats = result.get(searchPageShowType);
        	if (stats == null) {
        		stats = new SearchStats();
        		result.put(searchPageShowType, stats);
        	}
        	stats.setSearchCountRate(NumericUtil.getRateForPCT(Long.valueOf(stats.getSearchCount()), Long.valueOf(totalCount)));
	        stats.setDate(TimeUtils.applyDescForTime(timeType, time));
    		stats.setName(searchPageMap.get(searchPageShowType));
        }
		return result;
	}

	@Override
	public SearchStatsResult getSearchValueStats(Integer webId,
			Integer timeType, String time, Integer seId, Integer searchType,
			Integer conditionType, Integer startIndex, Integer offset) {
		SearchStatsResult result = new SearchStatsResult();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getSearchValueStats(webId, timeType, time, seId, searchType, conditionType, startIndex, offset);
		} else {
			result = offlineStatsService.getSearchValueStats(webId, timeType, time, seId, searchType, conditionType, startIndex, offset);
		}
		List<SearchStats> statsList = result.getStatsList();
		long totalCount = getTotalSearchCount(webId, timeType, time, seId, searchType);
		
		for(int i = 0; i< result.getStatsListSize(); i++){
			SearchStats stats = statsList.get(i);
			stats.setSearchCountRate(NumericUtil.getRateForPCT(stats.getSearchCount(), totalCount));
		}
		return result;
	}

	@Override
	public TopResponseTimeResult getTopResponseTimeResult(Integer webId,
			Integer timeType, String time, Integer seId, Integer searchType,
			Integer startIndex, Integer offset) {
		TopResponseTimeResult result = new TopResponseTimeResult();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getTopResponseTimeResult(webId, timeType, time, seId, searchType, startIndex, offset);
		} else {
			result = offlineStatsService.getTopResponseTimeResult(webId, timeType, time, seId, searchType, startIndex, offset);
		}
		return result;
	}

	@Override
	public SearchStatsResult getTopIpResult(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType, Integer startIndex,
			Integer offset) {
		SearchStatsResult result = new SearchStatsResult();
		boolean isRealTime = TimeUtils.isRealTime(timeType, time);
		if(isRealTime && timeType == DateType.DAY.getValue()){
			result = rtDayStatsService.getTopIpResult(webId, timeType, time, seId, searchType, startIndex, offset);
		} else{
			result = offlineStatsService.getTopIpResult(webId, timeType, time, seId, searchType, startIndex, offset);
		}
		List<SearchStats> statsList = result.getStatsList();
		long totalCount = getTotalSearchCount(webId, timeType, time, seId, searchType);
		
		for(int i = 0; i< result.getStatsListSize(); i++){
			SearchStats stats = statsList.get(i);
			stats.setSearchCountRate(NumericUtil.getRateForPCT(stats.getSearchCount(), totalCount));
		}
		return result;
	}
}
