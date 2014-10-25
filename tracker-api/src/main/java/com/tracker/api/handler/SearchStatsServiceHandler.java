package com.tracker.api.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.tracker.api.service.data.SiteSearchDataService;
import com.tracker.api.service.sitesearch.SiteSearchService;
import com.tracker.api.service.sitesearch.SiteSearchServiceImpl;
import com.tracker.api.thrift.search.SearchEngineParam;
import com.tracker.api.thrift.search.SearchStats;
import com.tracker.api.thrift.search.SearchStatsResult;
import com.tracker.api.thrift.search.SearchStatsService;
import com.tracker.api.thrift.search.TopResponseTimeResult;
import com.tracker.api.thrift.search.TopResponseTimeStats;
import com.tracker.api.util.TimeUtils;

/**
 * 提供站内搜索统计api服务， 主要包括kpi指标查询
 * 
 * 实现定义的thrift接口{@code SearchStatsService}
 * 
 * @author jason.hua
 *
 */
public class SearchStatsServiceHandler implements SearchStatsService.Iface{
	private Logger logger = LoggerFactory.getLogger(SearchStatsServiceHandler.class);
	
	private SiteSearchService searchStatsService = new SiteSearchServiceImpl();
	private SiteSearchDataService searchDataService = new SiteSearchDataService();
	private final static int NUM_TOP_RT_COUNT = 500;
	private final static int NUM_TOP_IP_COUNT = 500;
	private final static int NUM_TOP_SEARCH_VALUE_COUNT = 500;
	
	/**
	 * 用于网站首页
	 */
	@Override
	public Map<String, SearchStats> getSearchStats(int webId, int timeType,
			String time) throws TException {
		long startTime = System.currentTimeMillis();
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		try {
			time = TimeUtils.parseTime(timeType, time);
			for(Integer siteSeId: searchDataService.getSiteSearchEngine().keySet()){
				String searchEngine = searchDataService.getSiteSearchEngineName(siteSeId);
				Collection<Integer> searchTypeList = searchDataService.getSiteSearchType(siteSeId).keySet();
				if(searchTypeList.size() > 0){
					for(Integer searchType: searchTypeList){
						String sign = searchEngine + searchType;
						Map<String, SearchStats> resultMap = searchStatsService.getSearchStatsForDates(webId, timeType, Lists.newArrayList(time), siteSeId, searchType);
						if(resultMap.containsKey(time)){
							result.put(sign, resultMap.get(time));
						}
						logger.info("getSearchStats => " + searchEngine + "_" + searchType + " => " +  (System.currentTimeMillis() - startTime));
						startTime = System.currentTimeMillis();
					}
				} else {
					Map<String, SearchStats> resultMap = searchStatsService.getSearchStatsForDates(webId, timeType, Lists.newArrayList(time), siteSeId, null);
					if(resultMap.containsKey(time)){
						result.put(searchEngine, resultMap.get(time));
					}
					logger.info("getSearchStats => " + searchEngine  + " => " + (System.currentTimeMillis() - startTime));
					startTime = System.currentTimeMillis();
				}
			}
		} catch(Exception e){
			logger.error("error to getSearchStats", e);
		}
		return result;
	}
	
	/**
	 * 基于时间
	 */
	@Override
	public List<SearchStats> getSearchStatsForDate(int webId, int timeType,
			List<String> times, SearchEngineParam seParam) throws TException {
		List<SearchStats> result = new ArrayList<SearchStats>();
		try{
			if(times.size() < 2 || seParam.getSearchEngineId() <= 0)
				return result;
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			times = TimeUtils.parseTimes(timeType, times.get(0), times.get(1));
			if(times.size() > 0) {
				Map<String, SearchStats> resultMap = searchStatsService.getSearchStatsForDates(webId, timeType, times, seParam.getSearchEngineId(), searchType);
				result = new ArrayList<SearchStats>(resultMap.values());
				Collections.sort(result, new Comparator<SearchStats>() {
					@Override
					public int compare(SearchStats o1, SearchStats o2) {
						return o1.getName().compareTo(o2.getName());
					}
				});
			}
		} catch(Exception e){
			logger.error("error to getSearchStatsForDate", e);
		}
		return result;
	}
	
	@Override
	public long getTotalSearchCount(int webId, int timeType, String time,
			SearchEngineParam seParam) throws TException {
		long totalCount = 0;
		try{
			if(webId <= 0 || seParam.getSearchEngineId() <= 0)
				return totalCount;
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			time = TimeUtils.parseTime(timeType, time);
			totalCount = searchStatsService.getTotalSearchCount(webId, timeType, time, seParam.getSearchEngineId(), searchType);
		} catch(Exception e){
			logger.error("error to getTotalSearchCount", e);
		}
		return totalCount;
	}

	@Override
	public List<SearchStats> getSiteSearchStats(int webId, int timeType,
			String time, SearchEngineParam seParam, int resultType)
			throws TException {
		List<SearchStats> result = new ArrayList<SearchStats>();
		try{
			if(webId <= 0 || seParam.getSearchEngineId() <= 0)
				return result;
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			time = TimeUtils.parseTime(timeType, time);
			Map<String, SearchStats> resultMap = null;
			if(resultType == 0){
				resultMap = searchStatsService.getSearchConditionStats(webId, timeType, time, seParam.getSearchEngineId(), searchType);
			} else if(resultType <= 4) {
				resultMap = searchStatsService.getSearchResultStats(webId, timeType, time, seParam.getSearchEngineId(), searchType, resultType);
			}
			result = new ArrayList<SearchStats>(resultMap.values());
			if(resultType == 0){
				Collections.sort(result, new Comparator<SearchStats>(){
					@Override
					public int compare(SearchStats o1, SearchStats o2) {
						return (int) (o2.searchCount - o1.searchCount);
					}
				});
			} else {
				Collections.sort(result, new Comparator<SearchStats>(){
					@Override
					public int compare(SearchStats o1, SearchStats o2) {
						return o1.getFieldId() - o2.getFieldId();
					}
				});
			}
			
		} catch(Exception e){
			logger.error("error to getSiteSearchStats", e);
		}
		return result;
	}

	@Override
	public List<SearchStats> getSearchPageStats(int webId, int timeType,
			String time, SearchEngineParam seParam)
			throws TException {
		List<SearchStats> result = new ArrayList<SearchStats>();
		try{
			if(webId <= 0 || seParam.getSearchEngineId() <= 0)
				return result;
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			time = TimeUtils.parseTime(timeType, time);
			Map<String, SearchStats> resultMap = searchStatsService.getSearchPageStats(webId, timeType, time, seParam.getSearchEngineId(), searchType);
			result = new ArrayList<SearchStats>(resultMap.values());
			Collections.sort(result, new Comparator<SearchStats>(){
				@Override
				public int compare(SearchStats o1, SearchStats o2) {
					return (int) (o2.searchCount - o1.searchCount);
				}
			});
		} catch(Exception e){
			logger.error("error to getSiteSearchStats", e);
		}
		return result;
	}

	@Override
	public SearchStatsResult getSearchValueStats(int webId, int timeType,
			String time, SearchEngineParam seParam, int conditionType,
			int startIndex, int offset) throws TException {
		SearchStatsResult result = new SearchStatsResult(new ArrayList<SearchStats>(), 0);
		try{
			if(webId <= 0 || seParam.getSearchEngineId() <= 0)
				return result;
			if(startIndex > NUM_TOP_SEARCH_VALUE_COUNT){
				return result;
			}
			if(startIndex + offset > NUM_TOP_SEARCH_VALUE_COUNT){
				offset = NUM_TOP_SEARCH_VALUE_COUNT - startIndex;
			}
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			time = TimeUtils.parseTime(timeType, time);
			result = searchStatsService.getSearchValueStats(webId, timeType, time, seParam.getSearchEngineId(), searchType, conditionType, startIndex, offset);
			if(result.getTotalCount() > NUM_TOP_SEARCH_VALUE_COUNT)
				result.setTotalCount(NUM_TOP_SEARCH_VALUE_COUNT);
		} catch(Exception e){
			logger.error("error to getSiteSearchStats", e);
		}
		return result;
	}

	@Override
	public TopResponseTimeResult getTopResponseTimeResult(int webId,
			int timeType, String time, SearchEngineParam seParam,
			int startIndex, int offset) throws TException {
		TopResponseTimeResult result = new TopResponseTimeResult(new ArrayList<TopResponseTimeStats>(), 0);
		try{
			if(webId <= 0 || seParam.getSearchEngineId() <= 0)
				return result;
			if(startIndex > NUM_TOP_RT_COUNT){
				return result;
			}
			if(startIndex + offset > NUM_TOP_RT_COUNT){
				offset = NUM_TOP_RT_COUNT - startIndex;
			}
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			time = TimeUtils.parseTime(timeType, time);
			result = searchStatsService.getTopResponseTimeResult(webId, timeType, time, seParam.getSearchEngineId(), searchType, startIndex, offset);
			if(result.getTotalCount() > NUM_TOP_RT_COUNT)
				result.setTotalCount(NUM_TOP_RT_COUNT);
		} catch(Exception e){
			logger.error("error to getTopResponseTimeResult", e);
		}
		return result;
	}

	@Override
	public SearchStatsResult getTopIpResult(int webId, int timeType,
			String time, SearchEngineParam seParam, int startIndex, int offset)
			throws TException {
		SearchStatsResult result = new SearchStatsResult(new ArrayList<SearchStats>(), 0);
		try{
			if(webId <= 0 || seParam.getSearchEngineId() <= 0)
				return result;
			if(startIndex > NUM_TOP_IP_COUNT){
				return result;
			}
			if(startIndex + offset > NUM_TOP_IP_COUNT){
				offset = NUM_TOP_IP_COUNT - startIndex;
			}
			Integer searchType = seParam.getSearchType() <= 0? null: seParam.getSearchType();
			time = TimeUtils.parseTime(timeType, time);
			result = searchStatsService.getTopIpResult(webId, timeType, time, seParam.getSearchEngineId(), searchType, startIndex, offset);
			if(result.getTotalCount() > NUM_TOP_IP_COUNT)
				result.setTotalCount(NUM_TOP_IP_COUNT);
		} catch(Exception e){
			logger.error("error to getTopIpResult", e);
		}
		return result;
	}
}
