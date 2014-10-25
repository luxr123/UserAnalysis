package com.tracker.storm.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.tracker.common.cache.LocalCache;
import com.tracker.db.constants.AreaLevel;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.Geography;
import com.tracker.db.dao.data.model.SiteSearchCondition;
import com.tracker.db.dao.data.model.SiteSearchDefaultValue;
import com.tracker.db.dao.data.model.SiteSearchEngine;
import com.tracker.db.dao.data.model.SiteSearchPage;
import com.tracker.db.dao.data.model.UserTypeData;
import com.tracker.db.dao.data.model.VisitTypeData;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;

/**
 * 获取数据
 * @author jason.hua
 *
 */
public class DataService {
	private static Logger logger = LoggerFactory.getLogger(DataService.class);
	//网站
	private HBaseDao siteSEDao = null;
	private HBaseDao searchConDao = null;
	private HBaseDao searchDefaultValueDao = null;
	private HBaseDao searchPageDao = null;
	private HBaseDao userTypeDao = null;
	private HBaseDao visitTypeDao = null;
	private HBaseDao geoDao = null;
	
	private static final String VISIT_TYPE = "visitType";
	private static final String SITE_SEARCH_ENGINE = "siteSE";
	private static final String SITE_SEARCH_CONDITION = "siteSECondition";
	private static final String SEARCH_DEFAULT_VALUE= "searchDefaultValue";
	private static final String SEARCH_PAGE = "searchPage";
	private static final String USER_TYPE = "siteUserType";
	private static final String COUNTRY = "country"; //国家
	private static final String PROVINCE = "province"; //省份
	private static final String CITY = "city"; //城市

	
	private static LocalCache<String, Object> cache = new LocalCache<String, Object>(60 * 60); // 缓存1个小时
	private static HConnection hconnection = null;
	
	public DataService(HConnection hconnection){
		siteSEDao = new HBaseDao(hconnection, SiteSearchEngine.class);
		searchConDao = new HBaseDao(hconnection, SiteSearchCondition.class);
		searchDefaultValueDao = new HBaseDao(hconnection, SiteSearchDefaultValue.class);
		searchPageDao = new HBaseDao(hconnection, SiteSearchPage.class);
		userTypeDao = new HBaseDao(hconnection, UserTypeData.class);
		visitTypeDao = new HBaseDao(hconnection, VisitTypeData.class);
		geoDao = new HBaseDao(hconnection, Geography.class);
	}
	
	
	/**
	 * 获取国家名
	 */
	public Integer getCountrId(String country){
		Object value = cache.getOrElse(COUNTRY, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.COUNTRY.getValue());
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geoDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, null);
				Map<String, Integer> result = new HashMap<String, Integer>();
				for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
					result.put(keyResult.getT().getCountry(), RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX));
				}
				return result;
			}
		});
		Map<String, Integer> valueMap = (Map<String, Integer>)value;
		Integer countryId = valueMap.get(country);
		if(countryId == null)
			countryId = valueMap.get(Geography.COUNTRY_OTHER);
		return countryId;
	}
	
	/**
	 * 获取省份数据集合
	 */
	public Integer getProvinceId(final Integer countryId, String province){
		if(countryId == null)
			return null;
		Object value = cache.getOrElse(PROVINCE + "_" + countryId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.PROVINCE.getValue());
				QueryExtInfo queryExtInfo = new QueryExtInfo();
				Geography geography = new Geography();
				geography.setCountryId(countryId);
				queryExtInfo.setObj(geography);
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geoDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, queryExtInfo);
				Map<String, Integer> result = new HashMap<String, Integer>();
				for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
					result.put(keyResult.getT().getProvince(), RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX));
				}
				return result;
			}
		});
		Map<String, Integer> valueMap = (Map<String, Integer>)value;
		Integer provinceId = valueMap.get(province);
		if(provinceId == null)
			provinceId = valueMap.get(Geography.PROVINCE_OTHER);
		return provinceId;
	}

	/**
	 * 获取城市数据集合
	 */
	public Integer getCityId(final Integer countryId, final Integer provinceId, String city){
		if(countryId == null || provinceId == null)
			return null;
		
		Object value = cache.getOrElse(CITY + "_" + countryId + "_" + provinceId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				QueryExtInfo queryExtInfo = new QueryExtInfo();
				Geography geography = new Geography();
				geography.setCountryId(countryId);
				geography.setProvinceId(provinceId);
				queryExtInfo.setObj(geography);
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.CITY.getValue());
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geoDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, queryExtInfo);
				Map<String, Integer> result = new HashMap<String, Integer>();
				for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
					result.put(keyResult.getT().getCity(), RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX));
				}
				return result;
			}
		});
		Map<String, Integer> valueMap = (Map<String, Integer>)value;
		Integer cityId = valueMap.get(city);
		if(cityId == null)
			cityId = valueMap.get(Geography.CITY_OTHER);
		return cityId;
	}
	
	/**
	 * 根据搜索引擎名获取搜索引擎id
	 */
	public Integer getSearchEngineId(String category){
		Object value = cache.getOrElse(SITE_SEARCH_ENGINE, new Function<String, Object>(){
			@Override
			public Map<String, Integer> apply(String input) {
				Map<String, Integer> result = new HashMap<String, Integer>();
				List<SiteSearchEngine> seList = siteSEDao.findObjectListByRowPrefix(SiteSearchEngine.generateRowPrefix(), SiteSearchEngine.class, null);
				for(SiteSearchEngine se: seList){
					result.put(se.getName(), se.getSeId());
				}
				return result;
			}
		});
		Map<String, Integer> map = (Map<String, Integer>)value;
		Integer seId = map.get(category);
	    return  seId == null? -1: seId;
	}
	
	/**
	 * 获取搜索条件
	 */
	public Integer getSearchConditionType(final Integer siteSeId, final Integer searchType, String conField) {
		Object value = cache.getOrElse(SITE_SEARCH_CONDITION + "_" + siteSeId + "_" + (searchType == null? "":searchType), new Function<String, Object>(){
			@Override
			public Map<String, Integer> apply(String input) {
				Map<String, Integer> result = new HashMap<String, Integer>();
				List<SiteSearchCondition> seConList = searchConDao.findObjectListByRowPrefix(SiteSearchCondition.generateRowPrefix(siteSeId, searchType), SiteSearchCondition.class, null);
				for(SiteSearchCondition seCondition: seConList){
					result.put(seCondition.getField(), seCondition.getSeConType());
				}
				return result;
			}
		});
		Map<String, Integer> map = (Map<String, Integer>)value;
		Integer seConType = map.get(conField);
	    return  seConType == null? -1: seConType;
	}
	
	/**
	 * 获取需要过滤的默认搜索值
	 */
	public Map<String, String> getSearchDefaultValue(final Integer siteSeId, final Integer searchShowType) {
		Object value = cache.getOrElse(SEARCH_DEFAULT_VALUE + "_" + siteSeId + "_" + searchShowType, new Function<String, Object>(){
			@Override
			public Map<String, String> apply(String input) {
				Map<String, String> result = new HashMap<String, String>();
				List<SiteSearchDefaultValue> defaultValueList = searchDefaultValueDao.findObjectListByRowPrefix(SiteSearchDefaultValue.generateRowPrefix(siteSeId, searchShowType), SiteSearchDefaultValue.class, null);
				for(SiteSearchDefaultValue defaultValue: defaultValueList){
					result.put(defaultValue.getField(), defaultValue.getDefaultValue());
				}
				return result;
			}
		});
		return (Map<String, String>)value;
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
	
	/**
	 * 获取登录用户类型集合
	 */
	public boolean isLoginUser(final int webId, int userType){
		Object value = cache.getOrElse(USER_TYPE + "_login_" + webId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<UserTypeData> typeList = userTypeDao.findObjectListByRowPrefix(UserTypeData.generateRowPrefix(webId), UserTypeData.class, null);
				for(UserTypeData data: typeList){
					if(data.isLogin != null && data.isLogin > 0)
						result.put(data.getUserType(), data.getDesc());
				}
				return result;
			}
		});
		Map<Integer, String> map = (Map<Integer, String>)value;
		if(map.containsKey(userType)){
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * 获取用户类型集合<ch_name, type>
	 */
	public Map<Integer, String> getUserType(final int webId){
		Object value = cache.getOrElse(USER_TYPE + "_" + webId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<UserTypeData> typeList = userTypeDao.findObjectListByRowPrefix(UserTypeData.generateRowPrefix(webId), UserTypeData.class, null);
				for(UserTypeData data: typeList){
					result.put(data.getUserType(), data.getDesc());
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	/**
	 * 获取用户访问类型
	 */
	public Integer getVisitType(final int webId, final String sign, final Integer searchType){
		if(sign == null)
			return null;
		Object value = cache.getOrElse(VISIT_TYPE + "_" + webId + "_" + sign + "_" + (searchType ==null?"":searchType), new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				VisitTypeData data = visitTypeDao.findObject(VisitTypeData.generateRow(webId, sign, searchType), VisitTypeData.class, null);
				if(data != null)
					return data.getVisitType();
				else
					return 1;
			}
		});
		return (Integer)value;
	}
	
	public void close(){
		if(hconnection != null){
			try {
				hconnection.close();
			} catch (IOException e) {
				logger.warn("error to close HConnection", e);
			}
		}
	}
	
	public static void main(String[] args) {
		DataService service = new DataService(HbaseUtils.getHConnection("10.100.2.92,10.100.2.93"));
//		System.out.println(service.getSearchEngineId("FoxEngine"));
//		System.out.println(service.getSearchConditionType(1, 1, "area"));
//		System.out.println(service.getSearchDefaultValue(1, 1));
//		System.out.println(service.getSearchPageId(1, "searchmanager.php"));
//		System.out.println(service.getUserType(1));
		System.out.println(service.getVisitType(1, "visit", null));
		
//		System.out.println(service.getCountrId("中国"));
//		System.out.println(service.getProvinceId(1, "江苏省"));
//		System.out.println(service.getCityId(1, 5, "常州"));
	}
}
