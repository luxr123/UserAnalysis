package com.tracker.api.service.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.tracker.api.Servers;
import com.tracker.common.cache.LocalCache;
import com.tracker.db.constants.AreaLevel;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.Geography;
import com.tracker.db.dao.data.model.RefSearchEngine;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;

/**
 * 提供获取过滤数据服务
 * 主要包括如下业务：
 * 1. 国家数据
 * 2. 省份数据
 * 3. 城市数据
 * 4. 外部搜索引擎数据
 * @author jason.hua
 *
 */
public class FilterDataService {
	private HBaseDao geographyDao = new HBaseDao(Servers.hbaseConnection, Geography.class); //地域数据访问对象
	private HBaseDao refSEDao = new HBaseDao(Servers.hbaseConnection, RefSearchEngine.class); //外部搜索引擎数据访问对象

	private static final String COUNTRY = "country"; //国家
	private static final String PROVINCE = "province"; //省份
	private static final String CITY = "city"; //城市
	private static final String REF_SEARCH_ENGINE = "ref_search_engine";//外部链接-搜索引擎
	
	private static LocalCache<String, Object> cache = new LocalCache<String, Object>(3600); // 缓存1个小时
	
	/**
	 * 获取国家名
	 * @param countryId
	 * @return 如果数据库中countryId对应的数据不存在，则返回传入的countryId
	 */
	public String getCountryName(int countryId){
		String defaultName = "country:" + countryId;
		Object value = cache.getOrElse(COUNTRY, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.COUNTRY.getValue());
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geographyDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, null);
				Map<Integer, String> result = new HashMap<Integer, String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
						result.put(RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX), keyResult.getT().getCountry());
					}
				}
				return result;
			}
		});
		Map<Integer, String> idName = (Map<Integer, String>)value;
		String name = idName.get(countryId);
		return name == null ? defaultName: name;
	}
	
	/**
	 * 获取省份名字
	 * @param countryId
	 * @param provinceId
	 * @return 如果数据库中对应的数据不存在，则返回id值
	 */
	public String getProvinceName(final int countryId, int provinceId){
		String defaultName = "province:" + countryId + "_" + provinceId;
		Map<Integer, String> idName = getProvinceMap(countryId);
		String name = idName.get(provinceId);
		return name == null ? defaultName: name;
	}
	
	/**
	 * 获取省份数据集合
	 * @param countryId
	 * @return map<省份id,省份名>
	 */
	public Map<Integer, String> getProvinceMap(final int countryId){
		Object value = cache.getOrElse(PROVINCE + "_" + countryId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.PROVINCE.getValue());
				QueryExtInfo queryExtInfo = new QueryExtInfo();
				Geography geography = new Geography();
				geography.setCountryId(countryId);
				queryExtInfo.setObj(geography);
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geographyDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, queryExtInfo);
				Map<Integer, String> result = new HashMap<Integer, String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
						result.put(RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX), keyResult.getT().getProvince());
					}
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	/**
	 * 获取城市名字
	 */
	public String getCityName(int countryId, int provinceId, int cityId){
		String defaultName = "city:" + countryId + "_" + provinceId + "_" + cityId;
		Map<Integer, String> idName = getCityMap(countryId, provinceId);
		String name = idName.get(cityId);
		return name == null ? defaultName: name;
	}
	
	/**
	 * 获取城市数据集合
	 */
	public Map<Integer, String> getCityMap(final int countryId, final int provinceId){
		Object value = cache.getOrElse(CITY + "_" + countryId + "_" + provinceId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				QueryExtInfo queryExtInfo = new QueryExtInfo();
				Geography geography = new Geography();
				geography.setCountryId(countryId);
				geography.setProvinceId(provinceId);
				queryExtInfo.setObj(geography);
				String rowPrefix = Geography.generateRowPrefix(AreaLevel.CITY.getValue());
				List<SimpleHbaseDOWithKeyResult<Geography>> list = geographyDao.findObjectListAndKeyByRowPrefix(rowPrefix, Geography.class, queryExtInfo);
				Map<Integer, String> result = new HashMap<Integer, String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<Geography> keyResult: list){
						result.put(RowUtil.getRowIntField(keyResult.getRowKey(), Geography.ID_INDEX), keyResult.getT().getCity());
					}
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	/**
	 * 获取搜索引擎信息，用于访问来源过滤
	 */
	public Map<Integer, String> getRefSEData() {
		Object value = cache.getOrElse(REF_SEARCH_ENGINE + "_map<id, name>", new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = RefSearchEngine.generateRowPrefix();
				List<SimpleHbaseDOWithKeyResult<RefSearchEngine>> list = refSEDao.findObjectListAndKeyByRowPrefix(rowPrefix, RefSearchEngine.class, null);
				Map<Integer, String> result = new HashMap<Integer, String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<RefSearchEngine> keyResult: list){
						result.put(RowUtil.getRowIntField(keyResult.getRowKey(), RefSearchEngine.ID_INDEX), keyResult.getT().getName());
					}
				}
				return result;
			}
		});
		
		return (Map<Integer, String>)value;
	}
	
	public String getSEDomain(Integer seDomainId){
		return getRefSEData().get(seDomainId);
	}
	
	/**
	 * 获取搜索引擎域名集合
	 */
	public List<String> getRefSEDomains() {
		Object value = cache.getOrElse(REF_SEARCH_ENGINE + "_list<domain>", new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = RefSearchEngine.generateRowPrefix();
				List<SimpleHbaseDOWithKeyResult<RefSearchEngine>> list = refSEDao.findObjectListAndKeyByRowPrefix(rowPrefix, RefSearchEngine.class, null);
				List<String> domains = new ArrayList<String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<RefSearchEngine> keyResult: list){
						domains.add(keyResult.getT().getDomain());
					}
				}
				return domains;
				
				
			}
		});
		
		return (List<String>)value;
	}
	
	/**
	 * 获取搜索引擎名字
	 */
	public String getRefSEName(String domain){
		String defaultName = "其他";
		Object value = cache.getOrElse(REF_SEARCH_ENGINE + "_map<domain,name>", new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				String rowPrefix = RefSearchEngine.generateRowPrefix();
				List<SimpleHbaseDOWithKeyResult<RefSearchEngine>> list = refSEDao.findObjectListAndKeyByRowPrefix(rowPrefix, RefSearchEngine.class, null);
				Map<String, String> result = new HashMap<String, String>();
				if(list != null){
					for(SimpleHbaseDOWithKeyResult<RefSearchEngine> keyResult: list){
						result.put(keyResult.getT().domain, keyResult.getT().name);
					}
				}
				return result;
			}
		});
		
		Map<String, String> map = (Map<String, String>)value;
		String name = map.get(domain);
		return name == null ? defaultName: name;
	}
	
	
	public static void main(String[] args) {
		FilterDataService service = new FilterDataService();
		
//		System.out.println(service.getRefSEData());
//		System.out.println(service.getRefSEName("baidu.com"));
//		System.out.println(service.getRefSEDomain(1));
		
	}
}
