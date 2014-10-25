package com.tracker.hbase.service.sitesearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.constants.DateType;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.siteSearch.model.SearchConditionStats;
import com.tracker.db.dao.siteSearch.model.SearchDateStats;
import com.tracker.db.dao.siteSearch.model.SearchResultStats;
import com.tracker.db.simplehbase.request.PutRequest;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;
import com.tracker.hbase.service.util.HbaseUtil;
import com.tracker.hbase.service.util.TimeUtil;

/**
 * 
 * 文件名：SiteSearchCountImpl
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:47:38
 * 功能描述：SiteSearchCount接口实现类
 *
 */
public class SiteSearchCountImpl implements SiteSearchCount {
	private static final Logger logger = LoggerFactory.getLogger(SiteSearchCountImpl.class);
	
	private HBaseDao searchDataDao = new HBaseDao(HbaseUtil.getHConnection(), SearchDateStats.class);
	private HBaseDao searchResultDao = new HBaseDao(HbaseUtil.getHConnection(), SearchResultStats.class);
    private HBaseDao searchConditionDao = new HBaseDao(HbaseUtil.getHConnection(), SearchConditionStats.class);
	
	// 用于进行删除的对象
	private DeleteDataForSiteSearch delete = new DeleteDataForSiteSearchImpl();
	
	
	/**
	 * 函数名：getUnSummableKpiDao
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-20 下午3:50:35
	 * 功能描述：根据timeType获取相应的Dao（不可累加指标）
	 * @param timeType
	 * @return
	 */
	private UnSummableKpiDao getUnSummableKpiDao(Integer timeType) {
		UnSummableKpiDao unSummableKpiDao = null;
		if (timeType == DateType.WEEK.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtil.getHConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_WEEK_TABLE);
		} else if (timeType == DateType.MONTH.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtil.getHConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_MONTH_TABLE);
		} else if (timeType == DateType.YEAR.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtil.getHConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_YEAR_TABLE);
		}
		return unSummableKpiDao;
	}
	
	
	@Override
	public void countSearchStats(Integer webId, Integer timeType,
		String time, Integer seId, Integer searchType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteSearchStats(webId, timeType, time, seId, searchType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();

			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){
			     rows.add(SearchDateStats.generateRow(webId, searchTimeType, timeStr, seId, searchType));
		    }
			
			//获取数据   分类累加
			Map<String, SearchDateStats> result = new HashMap<String, SearchDateStats>();
			List<SimpleHbaseDOWithKeyResult<SearchDateStats>> list = searchDataDao.findObjectListAndKey(rows, SearchDateStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<SearchDateStats> rowResult: list){
				SearchDateStats stats = rowResult.getT();
				
				String key = time;  //只有一条记录
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			SearchDateStats stats = result.get(time);
			if (stats != null) {
				//更新不可累加数据
				Long ip = unSummableKpiDao.getSearchUnSummableKpiForDate(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_SE_DATE, seId, searchType);
				Long uv = unSummableKpiDao.getSearchUnSummableKpiForDate(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_SE_DATE, seId, searchType);
						
				stats.setIpCount(ip);
				stats.setUv(uv);
			}
			
			// 存储在hbase中
			List<PutRequest<SearchDateStats>> putRequestList = new ArrayList<PutRequest<SearchDateStats>>();
			for(String key: result.keySet()){
				 String row = SearchDateStats.generateRow(webId, timeType, time, seId, searchType);
				 putRequestList.add(new PutRequest<SearchDateStats>(row, result.get(key)));
			}
			searchDataDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countSearchStats error", e);
		}
	}

	@Override
	public void countSearchResultStats(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType, Integer resultType) {
		try {
			//如果有数据则删除
			delete.deleteSearchResultStats(webId, timeType, time, seId, searchType, resultType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();

			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){
			     rows.add(SearchResultStats.generateRequiredRowPrefix(webId, searchTimeType, timeStr, seId, searchType, resultType));
		    }
			
			//获取数据   分类累加
			Map<String, SearchResultStats> result = new HashMap<String, SearchResultStats>();
			List<SimpleHbaseDOWithKeyResult<SearchResultStats>> list = searchResultDao.findObjectListAndKeyByRowPrefixList(rows, SearchResultStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<SearchResultStats> rowResult: list){
				SearchResultStats stats = rowResult.getT();
				
				//按searchPage、searchShowType、typeValue三个字段分类累加
				String pageId = RowUtil.getRowField(rowResult.getRowKey(), SearchResultStats.SEARCH_PAGE_INDEX);
				String searchShowType = RowUtil.getRowField(rowResult.getRowKey(), SearchResultStats.SEARCH_SHOW_TYPE);
				String typeValue = RowUtil.getRowField(rowResult.getRowKey(), SearchResultStats.TYPE_VALUE_INDEX);
				
				String key = pageId + RowUtil.ROW_SPLIT + searchShowType  + RowUtil.ROW_SPLIT + typeValue; 
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			// 存储在hbase中
			List<PutRequest<SearchResultStats>> putRequestList = new ArrayList<PutRequest<SearchResultStats>>();
			for(String key: result.keySet()){
				 String row = SearchResultStats.generateRequiredRowPrefix(webId, timeType, time, seId, searchType, resultType) + key;
				 putRequestList.add(new PutRequest<SearchResultStats>(row, result.get(key)));
			}
			searchResultDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countSearchResultStats error", e);
		}
	}

	@Override
	public void countSearchConditionStats(Integer webId, Integer timeType,
			String time, Integer seId, Integer searchType) {
		try {
			//如果有数据则删除
			delete.deleteSearchConditionStats(webId, timeType, time, seId, searchType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();

			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){
			     rows.add(SearchConditionStats.generateRowPrefix(webId, searchTimeType, timeStr, seId, searchType));
		    }
			
			//获取数据   分类累加
			Map<String, SearchConditionStats> result = new HashMap<String, SearchConditionStats>();
			List<SimpleHbaseDOWithKeyResult<SearchConditionStats>> list = searchConditionDao.findObjectListAndKeyByRowPrefixList(rows, SearchConditionStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<SearchConditionStats> rowResult: list){
				SearchConditionStats stats = rowResult.getT();
				String key = RowUtil.getRowField(rowResult.getRowKey(), SearchConditionStats.SEARCH_CONDITION_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			// 存储在hbase中
			List<PutRequest<SearchConditionStats>> putRequestList = new ArrayList<PutRequest<SearchConditionStats>>();
			for(String key: result.keySet()){
				 String row = SearchConditionStats.generateRowPrefix(webId, timeType, time, seId, searchType) + key;
				 putRequestList.add(new PutRequest<SearchConditionStats>(row, result.get(key)));
			}
			searchConditionDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countSearchConditionStats error", e);
		}
	}


	/**
	 * 合并SearchConditionStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(SearchConditionStats result, SearchConditionStats stats) {
		result.setSearchCount(add(stats.getSearchCount(), result.getSearchCount()));
	}
	
	/**
	 * 合并SearchDateStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(SearchDateStats result, SearchDateStats stats) {
		result.setIpCount(add(stats.getIpCount(), result.getIpCount()));
		result.setMaxSearchCost(add(stats.getMaxSearchCost(), result.getMaxSearchCost()));
		result.setPageTurningCount(add(stats.getPageTurningCount(), result.getPageTurningCount()));
		result.setUv(add(stats.getUv(), result.getUv()));
		result.setSearchCount(add(stats.getSearchCount(), result.getSearchCount()));
		result.setTotalSearchCost(add(stats.getTotalSearchCost(), result.getTotalSearchCost()));
	}
	
	/**
	 * 合并SearchResultStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(SearchResultStats result, SearchResultStats stats) {
		result.setSearchCount(add(stats.getSearchCount(), result.getSearchCount()));
		result.setTotalSearchCost(add(stats.getTotalSearchCost(), result.getTotalSearchCost()));
	}
	
	
	/**
	 * 对两个Long类型的数进行加和
	 * @param num_1
	 * @param num_2
	 * @return
	 */
	public Long add(Long num_1, Long num_2) {
		Long sum = null;
		
		if ((num_1 != null) && (num_2 == null)) {
			sum = num_1;
		}
		if ((num_1 == null) && (num_2 != null)) {
			sum = num_2;
	    }
		if ((num_1 != null) && (num_2 != null)) {
			sum = num_1 + num_2;
	    }
		
		return sum;
	}
}
