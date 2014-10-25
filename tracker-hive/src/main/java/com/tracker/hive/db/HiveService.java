package com.tracker.hive.db;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.constants.AreaLevel;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.DateData;
import com.tracker.db.dao.data.model.Geography;
import com.tracker.db.dao.data.model.SiteSearchEngine;
import com.tracker.db.dao.data.model.SiteSearchPage;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;
import com.tracker.hive.Constants;

/**
 * hive 获取 hbase 表中数据
 * @author xiaorui.lu
 * 
 */
public class HiveService {
	static final Logger LOG = LoggerFactory.getLogger(HiveService.class);

	/**
	 * 加载日期时间并保存在 dateTimeCache中,即 日期 -> 日期唯一id
	 */
	public static Map<String, Integer> getDateTimeCache() {
		Map<String, Integer> dateTimeCache = new HashMap<String, Integer>();
		
		try {
			HBaseDao hbaseDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), DateData.class);
			
			Calendar cal = Calendar.getInstance();
			int year = cal.get(Calendar.YEAR);
			
			List<SimpleHbaseDOWithKeyResult<DateData>> list = hbaseDao.findObjectListAndKeyByRowPrefix(DateData.generateRowPrefix(year), DateData.class, null);
			for(SimpleHbaseDOWithKeyResult<DateData> rowObj: list){
				Integer id = RowUtil.getRowIntField(rowObj.getRowKey(), DateData.ID_INDEX);
				dateTimeCache.put(rowObj.getT().getDate(), id);
			}
		} catch (Exception e) {
			LOG.error("error to getDateTimeCache", e);
		}
		return dateTimeCache;
	}

	/**
	 * 加载地区值并保存在areaCache中,即通过 国家+省份+城市 -> 唯一id
	 */
	public static Map<String, Integer> getAreaCache() {
		Map<String, Integer> areaCache = new HashMap<String, Integer>();
		
		try {
			HBaseDao hbaseDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), Geography.class);
			
			QueryExtInfo extInfo = new QueryExtInfo();
			Geography geo = new Geography();
			geo.setLevel(AreaLevel.CITY.getValue());
			extInfo.setObj(geo);
			
			List<SimpleHbaseDOWithKeyResult<Geography>> list = hbaseDao.findObjectAndKeyList(Geography.class, extInfo);
			for(SimpleHbaseDOWithKeyResult<Geography> rowObj: list){
				Geography obj = rowObj.getT();
				areaCache.put(obj.getCountry()+obj.getProvince()+obj.getCity(), RowUtil.getRowIntField(rowObj.getRowKey(), Geography.ID_INDEX));
			}
		} catch (Exception e) {
			LOG.error("error to getAreaCache", e);
		}
		return areaCache;
	}

	/**
	 * 加载地区值并保存在areaArrayCache中,即通过 唯一id -> [国家,省份,城市]
	 */
	public static Map<Integer, String[]> getAreaFromIdCache() {
		Map<Integer, String[]> areaArrayCache = new HashMap<Integer, String[]>();
		try {
			HBaseDao hbaseDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), Geography.class);
			
			QueryExtInfo extInfo = new QueryExtInfo();
			Geography geo = new Geography();
			geo.setLevel(AreaLevel.CITY.getValue());
			extInfo.setObj(geo);
			
			List<SimpleHbaseDOWithKeyResult<Geography>> list = hbaseDao.findObjectAndKeyList(Geography.class, extInfo);
			for (SimpleHbaseDOWithKeyResult<Geography> rowObj : list) {
				Geography obj = rowObj.getT();
				String[] arr = { obj.getCountry(), obj.getProvince(), obj.getCity() };
				areaArrayCache.put(RowUtil.getRowIntField(rowObj.getRowKey(), Geography.ID_INDEX), arr);
			}
		} catch (Exception e) {
			LOG.error("error to getAreaCache", e);
		}
		return areaArrayCache;
	}

	/**
	 * 加载地区值并保存在countryProvCache中,即通过 唯一id -> 国家id+省份id
	 */
	public static Map<Integer, String> getCountryProvCache() {
		Map<Integer, String> countryProvCache = new HashMap<Integer, String>();
		
		try {
			HBaseDao hbaseDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), Geography.class);
			
			QueryExtInfo extInfo = new QueryExtInfo();
			Geography geo = new Geography();
			geo.setLevel(AreaLevel.CITY.getValue());
			extInfo.setObj(geo);
			
			List<SimpleHbaseDOWithKeyResult<Geography>> list = hbaseDao.findObjectAndKeyList(Geography.class, extInfo);
			for(SimpleHbaseDOWithKeyResult<Geography> rowObj: list){
				Geography obj = rowObj.getT();
				countryProvCache.put(RowUtil.getRowIntField(rowObj.getRowKey(), Geography.ID_INDEX), obj.getCountryId() + Constants.SPLIT + obj.getProvinceId());
			}
		} catch (Exception e) {
			LOG.error("error to getCountryProvCache", e);
		}
		return countryProvCache;
	}
	
	/**
	 * 函数名：getSearchPageCache
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 上午10:20:21
	 * 功能描述：加载搜索页面并保存在searchPageMap中, 即通过 webId -> map[SearchPage, pageId]
	 * @return
	 */
	public static Map<Integer, Map<String, Integer>> getSearchPageCache() {
		Map<Integer, Map<String, Integer>> searchPageMap = new HashMap<Integer, Map<String, Integer>>();
		
		try {
			HBaseDao searchPageDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), SiteSearchEngine.class);
			
			List<SimpleHbaseDOWithKeyResult<SiteSearchPage>> searchPageList = searchPageDao.findObjectListAndKeyByRowPrefix(SiteSearchPage.generateRowPrefix(), SiteSearchPage.class, null);
			for (SimpleHbaseDOWithKeyResult<SiteSearchPage> rowObj : searchPageList) {
				String row = rowObj.getRowKey();
				int webId = RowUtil.getRowIntField(row, SiteSearchPage.WEB_ID_INDEX);
				int pageId = RowUtil.getRowIntField(row, SiteSearchPage.SEARCH_PAGE_ID_INDEX);
				
				Map<String, Integer> pageIdMap = searchPageMap.get(webId);
				if (pageIdMap == null) {
					pageIdMap = new HashMap<String, Integer>();
					searchPageMap.put(webId, pageIdMap);
				}
				
				SiteSearchPage obj = rowObj.getT();
				pageIdMap.put(obj.getSearchPage(), pageId);
			}
		} catch (Exception e) {
			LOG.error("error to getSearchPage", e);
		}
		return searchPageMap;
	}

	/**
	 * 函数名：getSearchEngineCache
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 上午10:33:18
	 * 功能描述：加载SearchEngine并保存在searchEngineMap中, 即通过 name ->seId 
	 * @return
	 */
	public static Map<String, Integer> getSearchEngineCache() {
		Map<String, Integer> searchEngineMap = new HashMap<String, Integer>();
		
		try {
			HBaseDao searchEngineDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), SiteSearchPage.class);
			
			List<SimpleHbaseDOWithKeyResult<SiteSearchEngine>> searchEngineList = searchEngineDao.findObjectListAndKeyByRowPrefix(SiteSearchEngine.generateRowPrefix(), SiteSearchEngine.class, null);
			for (SimpleHbaseDOWithKeyResult<SiteSearchEngine> rowObj : searchEngineList) {
				SiteSearchEngine obj = rowObj.getT();
				searchEngineMap.put(obj.getName(), obj.getSeId());
			}
		} catch (Exception e) {
			LOG.error("error to getSearchPage", e);
		}
		return searchEngineMap;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(HiveService.getDateTimeCache());
		System.out.println(HiveService.getAreaCache());
		System.out.println(HiveService.getAreaFromIdCache());
		System.out.println(HiveService.getCountryProvCache());
		System.out.println(HiveService.getSearchEngineCache());
		System.out.println(HiveService.getSearchPageCache());
	}

}
