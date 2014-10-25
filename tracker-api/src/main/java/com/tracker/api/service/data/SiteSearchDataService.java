package com.tracker.api.service.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Function;
import com.tracker.api.Servers;
import com.tracker.common.cache.LocalCache;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.SiteSearchCondition;
import com.tracker.db.dao.data.model.SiteSearchEngine;
import com.tracker.db.dao.data.model.SiteSearchPage;
import com.tracker.db.dao.data.model.SiteSearchPageShowType;
import com.tracker.db.dao.data.model.SiteSearchType;
import com.tracker.db.dao.data.model.SiteSearchValue;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;

/**
 * 提供获取站内搜索数据服务
 * 主要包括如下业务：
 * 1. 站内搜索数据
 * 2. 站内搜索类型数据
 * 3. 站内搜索条件数据
 * 4. 站内搜索值数据
 * 5. 站内搜索展示类型数据
 * 
 * @author jason.hua
 *
 */
public class SiteSearchDataService {
	private HBaseDao siteSEDao = new HBaseDao(Servers.hbaseConnection, SiteSearchEngine.class); //搜索数据访问对象
	private HBaseDao siteSETypeDao = new HBaseDao(Servers.hbaseConnection, SiteSearchType.class); //搜索类型数据访问对象
	private HBaseDao siteSEConDao = new HBaseDao(Servers.hbaseConnection, SiteSearchCondition.class); //搜索条件数据访问对象
	private HBaseDao siteSearchValueDao = new HBaseDao(Servers.hbaseConnection, SiteSearchValue.class); //搜索值数据访问对象
	private HBaseDao siteSearchShowDao = new HBaseDao(Servers.hbaseConnection, SiteSearchPageShowType.class); //搜索展示类型访问对象
	private HBaseDao searchPageDao = new HBaseDao(Servers.hbaseConnection, SiteSearchPage.class);

	private static final String SEARCH_ENGINE = "siteSE"; //搜索数据标记
	private static final String SEARCH_TYPE = "siteSearchType"; //搜索类型数据标记
	private static final String SEARCH_CONDITION = "siteSECondition"; //搜索条件数据标记
	private static final String SEARCH_VALUE_NAME = "siteSearchValueName";//搜索值数据标记
	private static final String SEARCH_SHOW_TYPE = "searchShowType"; //搜索展示类型数据标记
	private static final String SEARCH_PAGE = "searchPage"; //搜索展示类型数据标记

	private static LocalCache<String, Object> cache = new LocalCache<String, Object>(60 * 60); // 缓存1个小时
	
	
	public Map<Integer, String> getSiteSearchEngine(){
		Object value = cache.getOrElse(SEARCH_ENGINE + "_map<id, name>", new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<SimpleHbaseDOWithKeyResult<SiteSearchEngine>> rowObjList = siteSEDao.findObjectListAndKeyByRowPrefix(SiteSearchEngine.generateRowPrefix(), SiteSearchEngine.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchEngine> rowObj: rowObjList){
					int seId = RowUtil.getRowIntField(rowObj.getRowKey(), SiteSearchEngine.SE_ID_INDEX);
					result.put(seId, rowObj.getT().getName());
				}
				return result;
			}
		});
		Map<Integer, String> map =  (Map<Integer, String>)value;
		return map;
	}
	
	/**
	 * 获取siteSId对应的站内搜索引擎名
	 */
	public String getSiteSearchEngineName(final int siteSeId){
		String defaultName = "site_se_name" + "_" + siteSeId;
		Map<Integer, String> map =  getSiteSearchEngine();
		String name = map.get(siteSeId);
		return name == null ? defaultName : name;
	}
	
	/**
	 * 获取搜索类型
	 */
	public Map<Integer, String> getSiteSearchType(final int siteSeId) {
		Object value = cache.getOrElse(SEARCH_TYPE + "_map<id, desc>" + siteSeId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<SimpleHbaseDOWithKeyResult<SiteSearchType>> rowObjList = siteSETypeDao.findObjectListAndKeyByRowPrefix(SiteSearchType.generateRowPrefix(siteSeId), SiteSearchType.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchType> rowObj: rowObjList){
					int seType = RowUtil.getRowIntField(rowObj.getRowKey(), SiteSearchType.SE_TYPE_INDEX);
					result.put(seType, rowObj.getT().getDesc());
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}

	/**
	 * 获取搜索条件, map<id, name>
	 */
	public Map<Integer, String> getSearchCondition(final Integer siteSeId, final Integer searchType) {
		Object value = cache.getOrElse(SEARCH_CONDITION + "_map<type,name>_" + siteSeId + "_" + searchType, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<SimpleHbaseDOWithKeyResult<SiteSearchCondition>> rowObjList = siteSEConDao.findObjectListAndKeyByRowPrefix(SiteSearchCondition.generateRowPrefix(siteSeId, searchType), SiteSearchCondition.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchCondition> rowObj: rowObjList){
					int seCondType = RowUtil.getRowIntField(rowObj.getRowKey(), SiteSearchCondition.SEARCH_CONDITION_INDEX);
					result.put(seCondType, rowObj.getT().getName());
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	public List<SiteSearchCondition> getSearchConditionList(final Integer siteSeId, final Integer searchType) {
		Object value = cache.getOrElse(SEARCH_CONDITION + "_list<obj>_" + siteSeId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				List<SiteSearchCondition> result = new ArrayList<SiteSearchCondition>();
				List<SiteSearchCondition> seConList = siteSEDao.findObjectListByRowPrefix(SiteSearchCondition.generateRowPrefix(siteSeId, searchType), SiteSearchCondition.class, null);
				for(SiteSearchCondition seCondition: seConList){
					result.add(seCondition);
				}
				return result;
			}
		});
		return (List<SiteSearchCondition>)value;
	}
	
	/**
	 * 获取搜索条件, <field, name>
	 */
//	public Map<String, String> getSearchConditionFields(final Integer siteSeId, final Integer searchType) {
//		Object value = cache.getOrElse(SITE_SEARCH_CONDITION + "_field_map_" + siteSeId, new Function<String, Object>(){
//			@Override
//			public Object apply(String input) {
//				Map<String, String> result = new HashMap<String, String>();
//				List<SiteSearchEngine> seConList = siteSEDao.findObjectListByRowPrefix(SiteSearchEngine.generateSEConRowPrefix(siteSeId, searchType), SiteSearchEngine.class, null);
//				for(SiteSearchEngine seCondition: seConList){
//					result.put(seCondition.getDesc(), seCondition.getName());
//				}
//				return result;
//			}
//		});
//		
//		return  (Map<String, String>)value;
//	}
	
//	 /**
//	  *  获取siteSId,seConId对应的站内搜索条件名 <name>
//	  */
//	public String getSearchConditionName(Integer siteSeId, Integer searchType, Integer seConId){
//		String defaultName = "conditionName:" + siteSeId + "_" + seConId;
//		Map<Integer, String> result = getSearchCondition(siteSeId, searchType);
//		String name = result.get(seConId);
//		return name == null ? defaultName : name;
//	}
	
//	/**
//	 * 获取搜索条件字段名, field
//	 */
//	public String getSearchConditionField(final Integer siteSeId, final Integer searchType, Integer seConId){
//		String defaultField = "conditionField:" + siteSeId + "_" + seConId;
//		Object value = cache.getOrElse(SITE_SEARCH_CONDITION + "_field_" + siteSeId + "_" + seConId, new Function<String, Object>(){
//			@Override
//			public Object apply(String input) {
//				Map<Integer, String> result = new HashMap<Integer, String>();
//				List<SiteSearchEngine> seConList = siteSEDao.findObjectListByRowPrefix(SiteSearchEngine.generateSEConRowPrefix(siteSeId, searchType), SiteSearchEngine.class, null);
//				for(SiteSearchEngine seCondition: seConList){
//					result.put(seCondition.getId(), seCondition.getDesc());
//				}
//				return result;
//			}
//		});
//		
//		Map<Integer, String> result = (Map<Integer, String>)value;
//		String field = result.get(seConId);
//		return field == null ? defaultField : field;
//	}
	
	/**
	 * 获取搜索值名
	 */
	public String getSearchValueName(final int siteSeId, final int conType, String valueIdStr){
		if(valueIdStr == null)
			return "null error";
		Object value = cache.getOrElse(SEARCH_VALUE_NAME + "_" + siteSeId + "_" + conType, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<String, String> result = new HashMap<String, String>();
				List<SimpleHbaseDOWithKeyResult<SiteSearchValue>> searchDataList = siteSearchValueDao.findObjectListAndKeyByRowPrefix(SiteSearchValue.generateRowPrefix(siteSeId, conType), SiteSearchValue.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchValue> rowSearchData: searchDataList){
					result.put(RowUtil.getRowField(rowSearchData.getRowKey(), SiteSearchValue.SITE_SEARCH_VALUE_INDEX), rowSearchData.getT().getName_ch());
				}
				return result;
			}
		});
		Map<String, String> map = (Map<String, String>)value;
		if(map.size() == 0)
			return valueIdStr;
		String[] strs = valueIdStr.split(",");
		for(int i = 0; i < strs.length; i++){
			String name = map.get(strs[i]);
			if(name != null){
				strs[i] = name;
			}
		}
		return StringUtils.join(strs, ",");
	}
	
	/**
	 * 获取搜索页上展示类型集合
	 * @return
	 */
	public Map<String, String> getSearchShowTypeMap(final int webId, final int seId, final Integer searchType) {
		Object value = cache.getOrElse(SEARCH_SHOW_TYPE + "_" + webId  + "_" + seId + "_" + (searchType ==null?"":searchType), new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<String, String> result = new HashMap<String, String>();
				List<SimpleHbaseDOWithKeyResult<SiteSearchPageShowType>> rowObjList = siteSearchShowDao.findObjectListAndKeyByRowPrefix(SiteSearchPageShowType.generateRowPrefix(webId, seId, searchType), SiteSearchPageShowType.class, null);
				for(SimpleHbaseDOWithKeyResult<SiteSearchPageShowType> rowObj: rowObjList){
					String searchPage = RowUtil.getRowField(rowObj.getRowKey(), SiteSearchPageShowType.SEARCH_PAGE_INDEX);
					int showType = RowUtil.getRowIntField(rowObj.getRowKey(), SiteSearchPageShowType.SHOW_TYPE_INDEX); 
					result.put(searchPage + "-" + showType, searchPage + "-" + rowObj.getT().getName());
				}
				return result;
			}
		});
		Map<String, String> valueMap = (Map<String, String>)value;
		return valueMap;
	}
	
	/**
	 * 获取搜索页面集合
	 */
	public Integer getSearchPageId(final int webId, String searchPage) {
		Object value = cache.getOrElse(SEARCH_PAGE + "_" + webId, new Function<String, Object>(){
			@Override
			public Map<String, Integer> apply(String input) {
				Map<String, Integer> result = new HashMap<String, Integer>();
				List<SimpleHbaseDOWithKeyResult<SiteSearchPage>> searchPageList = searchPageDao.findObjectListAndKeyByRowPrefix(
						SiteSearchPage.generateRowPrefix(webId), SiteSearchPage.class, null);
				for (SimpleHbaseDOWithKeyResult<SiteSearchPage> rowObj : searchPageList) {
					SiteSearchPage obj = rowObj.getT();
					String row = rowObj.getRowKey();
					int pageId = RowUtil.getRowIntField(row, SiteSearchPage.SEARCH_PAGE_ID_INDEX);
					result.put(obj.getSearchPage(), pageId);
				}
				return result;
			}
		});
		Map<String, Integer> map = (Map<String, Integer>)value;
		Integer searchPageId = map.get(searchPage);
	    return  searchPageId == null? -1: searchPageId;
	}
	
	
	
	public static void main(String[] args) {
		SiteSearchDataService service = new SiteSearchDataService();
		
//		System.out.println(service.getWebSite());
//		System.out.println(service.getSiteSearchEngineName(1));
//		System.out.println(service.getSiteSearchType(1));
		System.out.println(service.getSearchCondition(1, 3));
//		System.out.println(service.getSearchConditionName(1, 1));
//		System.out.println(service.getSearchPage(1, 1));
//		System.out.println(service.getSearchShowType(1, 1));
//		
//		System.out.println(service.getUserType(1));
//		
//		System.out.println(service.getUserTypeEnName(1, 1));
		
//		System.out.println(service.getSearchShowTypeMap(1, 1, 1));
	}
}
