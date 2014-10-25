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
import com.tracker.common.utils.DateUtil;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.SearchConditionStats;
import com.tracker.db.dao.siteSearch.model.SearchDateStats;
import com.tracker.db.dao.siteSearch.model.SearchResultStats;
import com.tracker.db.dao.siteSearch.model.TopSearchIpStats;
import com.tracker.db.dao.siteSearch.model.TopSearchRtStats;
import com.tracker.db.dao.siteSearch.model.TopSearchValueStats;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;

/**
 * 搜索行为分析离线数据服务(日、周、月、年）
 * @author jason.hua
 *
 */
public class SiteSearchOfflineServiceImpl implements SiteSearchService{
	private HBaseDao searchResultDao = new HBaseDao(Servers.hbaseConnection, SearchResultStats.class);
	private HBaseDao searchConditionDao = new HBaseDao(Servers.hbaseConnection, SearchConditionStats.class);
	private HBaseDao searchValueDao = new HBaseDao(Servers.hbaseConnection, TopSearchValueStats.class);
	private HBaseDao searchDataDao = new HBaseDao(Servers.hbaseConnection, SearchDateStats.class);
	private HBaseDao topRTDao = new HBaseDao(Servers.hbaseConnection, TopSearchRtStats.class);
	private HBaseDao topIPDao = new HBaseDao(Servers.hbaseConnection, TopSearchIpStats.class);
	private SiteSearchDataService searchDataService = new SiteSearchDataService();
	
	/**
	 * 获取总的搜索次数
	 */
	public long getTotalSearchCount(Integer webId, Integer timeType, String time, Integer seId, Integer searchType)  {
		String rowPrefix = SearchResultStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType());
		List<SearchResultStats> list = searchResultDao.findObjectListByRowPrefix(rowPrefix, SearchResultStats.class, null);
		
		long totalSearchCount = 0;
		for(SearchResultStats stats: list){
			totalSearchCount += stats.getSearchCount();
		}
		return totalSearchCount;
	}

	/**
	 * 获取基于时间的统计数据
	 */
	public Map<String, SearchStats> getSearchStatsForDates(Integer webId, Integer timeType, List<String> times, Integer seId, Integer searchType) {
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		//创建rowPrefix，获取list
		List<String> rowList = new ArrayList<String>();
		for(String time: times){
	        String row = SearchDateStats.generateRow(Integer.valueOf(webId), Integer.valueOf(timeType), time, seId, searchType);
	        rowList.add(row);
		}
		List<SimpleHbaseDOWithKeyResult<SearchDateStats>> list = searchDataDao.findObjectListAndKey(rowList, SearchDateStats.class, null);
		for(SimpleHbaseDOWithKeyResult<SearchDateStats> rowStats: list){
			SearchStats stats = new SearchStats();
			String date = RowUtil.getRowField(rowStats.getRowKey(), SearchDateStats.TIME_INDEX);
			SearchDateStats obj = rowStats.getT();
			stats.setSearchCount(obj.getSearchCount() == null?  0: obj.getSearchCount());
			stats.setIpCount(obj.getIpCount() == null? 0: obj.getIpCount());
			stats.setSearchUserCount(obj.getUv() == null? 0: obj.getUv());
			stats.setMaxSearchCost(obj.getMaxSearchCost() == null? 0: obj.getMaxSearchCost());
	        stats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(obj.getTotalSearchCost(), obj.getSearchCount()));
	        result.put(date, stats);
		}
		
		final Map<String, Long> pageTurnCountMap = getPageTurningCount(webId, timeType, times, seId, searchType);
		for(String time : pageTurnCountMap.keySet()){
			SearchStats stats = result.get(time);
			if(stats == null)
				continue;
			Long pageTurningCount = pageTurnCountMap.get(time);
			stats.setPageTurningCount(pageTurningCount);
		}
		return result;
	}
	
	/**
	 * 获取基于展示页码、搜索结果数、响应时间、搜索时段的统计数据
	 */
	public Map<String, SearchStats> getSearchResultStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType, Integer resultType){
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		//创建rowPrefix
		String rowPrefix = SearchResultStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, resultType);
		List<SimpleHbaseDOWithKeyResult<SearchResultStats>> list = searchResultDao.findObjectListAndKeyByRowPrefix(rowPrefix, SearchResultStats.class, null);
		
		//计算总搜索次数
		for(SimpleHbaseDOWithKeyResult<SearchResultStats> rowStats: list){
			String name = RowUtil.getRowField(rowStats.getRowKey(), SearchResultStats.TYPE_VALUE_INDEX);
			SearchResultStats obj = rowStats.getT();
			SearchStats originStats = result.get(name);
			if(originStats == null){
				SearchStats stats = new SearchStats();
				stats.setFieldId(RowUtil.getRowIntField(rowStats.getRowKey(), SearchResultStats.TYPE_VALUE_INDEX));
				stats.setSearchCount(obj.getSearchCount());
				stats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(obj.getTotalSearchCost(), obj.getSearchCount()));
				result.put(name, stats);
			} else {
				long totalCost = originStats.getAvgSearchCost() * originStats.getSearchCount() + obj.getTotalSearchCost();
				originStats.setSearchCount(obj.getSearchCount() + originStats.getSearchCount());
				originStats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(totalCost, obj.getSearchCount()));
			}
		}
		return result;
	}

	/**
	 * 获取基于搜索条件的统计数据
	 */
	public Map<String, SearchStats> getSearchConditionStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType){
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
		//创建rowPrefix
		String rowKeyPrefix = SearchConditionStats.generateRowPrefix(webId, timeType, time, seId, searchType);
		List<SimpleHbaseDOWithKeyResult<SearchConditionStats>> list = searchConditionDao.findObjectListAndKeyByRowPrefix(rowKeyPrefix, SearchConditionStats.class, null);
		//获取SearchStats
		Map<String, SearchStats> statsMap = new HashMap<String, SearchStats>();
		for(SimpleHbaseDOWithKeyResult<SearchConditionStats> rowStats: list){
			SearchStats stats = new SearchStats();
			int seConId = RowUtil.getRowIntField(rowStats.getRowKey(), SearchConditionStats.SEARCH_CONDITION_INDEX);
//			String field = searchDataService.getSearchConditionField(seId, searchType, seConId);
			stats.setSearchCount(rowStats.getT().getSearchCount());
			statsMap.put(String.valueOf(seConId), stats);
		}
		return result;
	}

	/**
	 * 获取基于搜索页面的统计数据
	 */
	public Map<String, SearchStats> getSearchPageStats(Integer webId, Integer timeType, String time, Integer seId, Integer searchType){
		Map<String, SearchStats> result = new HashMap<String, SearchStats>();
	    String rowPrefix = SearchResultStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, SearchResultType.SEARCH_COST.getType());
	    List<SimpleHbaseDOWithKeyResult<SearchResultStats>> list = searchResultDao.findObjectListAndKeyByRowPrefix(rowPrefix, SearchResultStats.class, null);

		//计算总搜索次数
		for(SimpleHbaseDOWithKeyResult<SearchResultStats> rowStats: list){
			String searchPage = RowUtil.getRowField(rowStats.getRowKey(), SearchResultStats.SEARCH_PAGE_INDEX) ;
			Integer showType = RowUtil.getRowIntField(rowStats.getRowKey(), SearchResultStats.SEARCH_SHOW_TYPE);
			String ch_name = searchPage + "-" + showType;
			SearchResultStats obj = rowStats.getT();
			SearchStats originStats = result.get(ch_name);
			if(originStats == null){
				SearchStats stats = new SearchStats();
				stats.setSearchCount(obj.getSearchCount());
				stats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(obj.getTotalSearchCost(), obj.getSearchCount()));
				result.put(ch_name, stats);
			} else {
				long totalCost = originStats.getAvgSearchCost() * originStats.getSearchCount() + obj.getTotalSearchCost();
				originStats.setSearchCount(obj.getSearchCount() + originStats.getSearchCount());
				originStats.setAvgSearchCost(NumericUtil.getAvgTimeForMilliSec(totalCost, obj.getSearchCount()));
			}
		}
	    return result;
	}
	
	/**
	 * 获取基于搜索值的统计数据
	 */
	public SearchStatsResult getSearchValueStats(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType, Integer conditionType, Integer startIndex, Integer offset){
		SearchStatsResult result = new SearchStatsResult(new ArrayList<SearchStats>(), 0);
		String rowKeyPrefix = TopSearchValueStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, conditionType);
	
		//获取统计指标
		long totalCount = getTotalSearchCount(webId, timeType, time, seId, searchType);
		
		QueryExtInfo<TopSearchValueStats> queryInfo = new QueryExtInfo<TopSearchValueStats>();
		queryInfo.setLimit(startIndex, offset);
		List<SimpleHbaseDOWithKeyResult<TopSearchValueStats>> list = searchValueDao.findObjectListAndKeyByRowPrefix(rowKeyPrefix, TopSearchValueStats.class, queryInfo);
		
		for(SimpleHbaseDOWithKeyResult<TopSearchValueStats> rowStats: list){
			SearchStats stats = new SearchStats();
			int conType =  RowUtil.getRowIntField(rowStats.getRowKey(), TopSearchValueStats.SEARCH_CONDITION_INDEX);
			String valueIdStr = RowUtil.getRowField(rowStats.getRowKey(), TopSearchValueStats.SEARCH_VALUE_INDEX);
			stats.setName(searchDataService.getSearchValueName(seId, conType, valueIdStr));
			stats.setDate(TimeUtils.applyDescForTime(timeType, time));
			stats.setSearchCount(rowStats.getT().getSearchCount());
			//获取统计指标
    		stats.setSearchCountRate(NumericUtil.getRateForPCT(Long.valueOf(stats.getSearchCount()), Long.valueOf(totalCount)));
    		result.addToStatsList(stats);
		}
		result.setTotalCount(searchValueDao.countByRowPrefix(rowKeyPrefix));
		return result;
	}
	
	/**
	 * 获取top最慢响应时间记录
	 */
	public TopResponseTimeResult getTopResponseTimeResult(Integer webId,
			Integer timeType, String time, Integer seId, Integer searchType, Integer startIndex, Integer offset){
		TopResponseTimeResult result = new TopResponseTimeResult(new ArrayList<TopResponseTimeStats>(), 0);
		String rowPrefix = TopSearchRtStats.generateRowPrefix(webId, timeType, time, seId, searchType);

		//total count
		long totalCount = topRTDao.countByRowPrefix(rowPrefix, TopSearchRtStats.class, null);
		result.setTotalCount(totalCount);
	
		//data
		QueryExtInfo<TopSearchRtStats> queryInfo = new QueryExtInfo<TopSearchRtStats>();
		queryInfo.setLimit(startIndex, offset);
		List<TopSearchRtStats> list = topRTDao.findObjectListByRowPrefix(rowPrefix, TopSearchRtStats.class, queryInfo);
		for(TopSearchRtStats stats: list){
			TopResponseTimeStats rtStats = new TopResponseTimeStats();
			if(stats.getUserId() != null)
				rtStats.setUserId(stats.getUserId());
			if(stats.getUserType() != null)
				rtStats.setUserType(stats.getUserType());
			rtStats.setCookieId(stats.getCookieId());
			rtStats.setIp(stats.getIp());
			rtStats.setVisitTime(TimeUtils.parseTimeToSecond(stats.getServerTime()).split(" ")[1]);
			if(stats.getResponseTime() != null)
				rtStats.setResponseTime(stats.getResponseTime());
			if(stats.getTotalResultCount() != null)
				rtStats.setTotalResultCount(stats.getTotalResultCount());
			rtStats.setSearchValueStr(stats.getSearchValueStr());
			result.addToStatsList(rtStats);
		}
		return result;
	}
	
	/**
	 * 获取top最慢响应时间记录
	 */
	public SearchStatsResult getTopIpResult(Integer webId,
			Integer timeType, String time, Integer searchEngineId, Integer searchType, Integer startIndex, Integer offset){
		SearchStatsResult result = new SearchStatsResult(new ArrayList<SearchStats>(), 0);
		String rowPrefix = TopSearchIpStats.generateRowPrefix(webId, timeType, time, searchEngineId, searchType);
		QueryExtInfo<TopSearchIpStats> queryInfo = new QueryExtInfo<TopSearchIpStats>();
		queryInfo.setLimit(startIndex, offset);
		List<TopSearchIpStats> list = topIPDao.findObjectListByRowPrefix(rowPrefix, TopSearchIpStats.class, queryInfo);
		long totalCount = getTotalSearchCount(webId, timeType, time, searchEngineId, searchType);
		for(TopSearchIpStats stats: list){
			SearchStats searchStats = new SearchStats();
			searchStats.setDate(TimeUtils.applyDescForTime(timeType, time));
			searchStats.setName(stats.getIp());
			searchStats.setSearchCount(stats.getSearchCount());
			searchStats.setSearchCountRate(NumericUtil.getRateForPCT(stats.getSearchCount(), totalCount));
			result.addToStatsList(searchStats);
		}
		result.setTotalCount(topIPDao.countByRowPrefix(rowPrefix));
		return result;
	}
	
	/**
	 * <date, pageTurningCount>
	 */
	private Map<String, Long> getPageTurningCount(int webId, int timeType, List<String> times, Integer seId, Integer searchType){
		Map<String, Long> result = new HashMap<String, Long>();
		//创建rowPrefix，获取list
		List<String> rowPrefixList = new ArrayList<String>();
		
		for(String time : times){
			String requiredRowPrefix = SearchResultStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, SearchResultType.DISPLAY_PAGE_NUM.getType());
			rowPrefixList.add(requiredRowPrefix);
		}
	      
		List<SimpleHbaseDOWithKeyResult<SearchResultStats>> list = searchResultDao.findObjectListAndKeyByRowPrefixList(rowPrefixList, SearchResultStats.class, null);
		for(SimpleHbaseDOWithKeyResult<SearchResultStats> rowObj: list){
			String date = RowUtil.getRowField(rowObj.getRowKey(), SearchResultStats.TIME_INDEX);
			int typeValue = RowUtil.getRowIntField(rowObj.getRowKey(), SearchResultStats.TYPE_VALUE_INDEX);
			if(typeValue != SearchPageNumType.ONE.getType()){
				Long originValue = result.get(date);
				if(originValue == null)
					originValue = 0L;
				originValue += 1L;
				result.put(date, originValue);
			}
		}
		return result;
	}
}
