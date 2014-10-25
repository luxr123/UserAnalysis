package com.tracker.api.service.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.tracker.api.Servers;
import com.tracker.common.cache.LocalCache;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.Page;
import com.tracker.db.dao.data.model.UserTypeData;
import com.tracker.db.dao.data.model.VisitTypeData;
import com.tracker.db.dao.data.model.WebSite;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;

/**
 * 获取过滤数据
 * @author jason.hua
 *
 */
public class WebSiteDataService {
	//网站
	private HBaseDao webSiteDao = new HBaseDao(Servers.hbaseConnection, WebSite.class);
	private HBaseDao userTypeDao = new HBaseDao(Servers.hbaseConnection, UserTypeData.class);
	private HBaseDao visitTypeDao = new HBaseDao(Servers.hbaseConnection, VisitTypeData.class);
	private HBaseDao pageDao = new HBaseDao(Servers.hbaseConnection, Page.class);
	
	private static final String WEBSITE = "website";
	private static final String PAGE = "page";
	private static final String USER_TYPE = "userType";
	private static final String VISIT_TYPE = "visitType";
	
	private static LocalCache<String, Object> cache = new LocalCache<String, Object>(60 * 60); // 缓存1个小时
	
	/**
	 * 获取网站数据
	 * @return
	 */
	public Map<Integer, String> getWebSite(){
		Object value = cache.getOrElse(WEBSITE, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<WebSite>  webSiteList = webSiteDao.findObjectListByRowPrefix(WebSite.generateRowPrefix(), WebSite.class, null);
				for(WebSite webSite: webSiteList){
					result.put(webSite.getId(), webSite.getDomain());
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	public String getWebSiteUrlPrefix(final int webId){
		Object value = cache.getOrElse(WEBSITE + "_urlPrefix", new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<WebSite>  webSiteList = webSiteDao.findObjectListByRowPrefix(WebSite.generateRowPrefix(), WebSite.class, null);
				for(WebSite webSite: webSiteList){
					result.put(webSite.getId(), webSite.getUrlPrefix());
				}
				return result;
			}
		});
		Map<Integer, String> valueMap = (Map<Integer, String>) value;
		String urlPrefix = valueMap.get(webId);
		return urlPrefix != null? urlPrefix: "http://jingying/";
	}
	
	/**
	 * 根据pageSign获取url描述
	 * @param pageId
	 * @return
	 */
	public String getPageDesc(final int webId, String pageSign){
		Object value = cache.getOrElse(PAGE + "_map<pageSign, desc>_" + webId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<String, String> result = new HashMap<String, String>();
				List<SimpleHbaseDOWithKeyResult<Page>> list = pageDao.findObjectListAndKeyByRowPrefix(Page.generateRowPrefix(webId), Page.class, null);
				for(SimpleHbaseDOWithKeyResult<Page> rowObj: list){
					result.put(RowUtil.getRowField(rowObj.getRowKey(), Page.PAGE_SIGN_INDEX), rowObj.getT().getPageDesc());
				}
				return result;
			}
		});
		Map<String, String> pageMap = (Map<String, String>)value;
		if(pageSign == null || !pageMap.containsKey(pageSign)){
			return "其他页面";
		} else {
			return pageMap.get(pageSign);
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
	 * 获取用户类型集合
	 */
	public String getUserTypeName(final int webId, int userType){
		Map<Integer, String> map =  getUserType(webId);
		String name = map.get(userType);
		return name == null ? "other": name;
	}
	
	/**
	 * 获取用户类型集合
	 */
	public String getUserTypeEnName(final int webId, int userType){
		Object value = cache.getOrElse(USER_TYPE + "_en_name_" + webId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<UserTypeData> typeList = userTypeDao.findObjectListByRowPrefix(UserTypeData.generateRowPrefix(webId), UserTypeData.class, null);
				for(UserTypeData data: typeList){
					result.put(data.getUserType(), data.getEn_name());
				}
				return result;
			}
		});
		Map<Integer, String> map =  (Map<Integer, String>)value;
		String name = map.get(userType);
		return name == null ? "other": name;
	}
	
	/**
	 * 获取登录用户类型集合
	 */
	public Map<Integer, String> getLoginUserType(final int webId){
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
		return (Map<Integer, String>)value;
	}
	
	/**
	 * 获取登录用户类型集合
	 */
	public boolean isLoginUser(final int webId, int userType){
		Map<Integer, String> map = getLoginUserType(webId);
		if(map.containsKey(userType)){
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * 获取用户访问类型集合
	 */
	public Map<Integer, String> getVisitType(final int webId){
		Object value = cache.getOrElse(VISIT_TYPE + "_" + webId, new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				Map<Integer, String> result = new HashMap<Integer, String>();
				List<VisitTypeData> typeList = visitTypeDao.findObjectListByRowPrefix(VisitTypeData.generateRowPrefix(webId), VisitTypeData.class, null);
				for(VisitTypeData data: typeList){
					result.put(data.getVisitType(), data.getDesc());
				}
				return result;
			}
		});
		return (Map<Integer, String>)value;
	}
	
	public int getVisitTypeOfSearch(final int webId, final String searchEngine, final Integer searchType) {
		Object value = cache.getOrElse(VISIT_TYPE + "_" + webId + "_" + searchEngine + "_" + (searchType ==null?"":searchType), new Function<String, Object>(){
			@Override
			public Object apply(String input) {
				VisitTypeData data = visitTypeDao.findObject(VisitTypeData.generateRow(webId, searchEngine, searchType), VisitTypeData.class, null);
				if(data != null)
					return data.getVisitType();
				else
					return -1;
			}
		});
		return (Integer)value;
	}
	
	public static void main(String[] args) {
		WebSiteDataService service = new WebSiteDataService();
		
//		System.out.println(service.getWebSite());
//		System.out.println(service.getSiteSearchEngineName(1));
//		System.out.println(service.getSiteSearchType(1));
//		System.out.println(service.getSearchCondition(1));
//		System.out.println(service.getSearchConditionName(1, 1));
//		System.out.println(service.getSearchPage(1, 1));
//		System.out.println(service.getSearchShowType(1, 1));
//		
//		System.out.println(service.getUserType(1));
//		
//		System.out.println(service.getUserTypeEnName(1, 1));
//		System.out.println(service.getVisitType(1));
//		System.out.println(service.getVisitTypeOfSearch(1, "CaseEngine", 1));
		
//		System.out.println(service.getVisitTypeChName(1, 1));
//		System.out.println(service.isLoginUser(1, 1));
		
//		System.out.println(service.getWebSiteUrlPrefix(1));
		
		System.out.println(service.getPageDesc(1, "index.php"));
	}
}
