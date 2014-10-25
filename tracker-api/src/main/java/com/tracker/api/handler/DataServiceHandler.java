package com.tracker.api.handler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.api.service.data.FilterDataService;
import com.tracker.api.service.data.SiteSearchDataService;
import com.tracker.api.service.data.WebSiteDataService;
import com.tracker.api.thrift.data.DataService;
import com.tracker.api.thrift.data.FilterEntry;
import com.tracker.api.thrift.data.SEFilterEntry;
import com.tracker.db.dao.data.model.SiteSearchCondition;

/**
 * 提供站内过滤数据获取服务
 * 
 * 实现定义的thrift接口{@code DataService}
 * 
 * @author jason.hua
 *
 */
public class DataServiceHandler implements DataService.Iface{
	private Logger logger = LoggerFactory.getLogger(DataServiceHandler.class);

	private FilterDataService filterDataService = new FilterDataService();
	private WebSiteDataService webSiteDataService = new WebSiteDataService();
	private SiteSearchDataService searchDataService = new SiteSearchDataService();

	
	@Override
	public List<FilterEntry> getWebSite() throws TException {
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		try{
			result = parseData(webSiteDataService.getWebSite());
		} catch(Exception e){
			logger.error("error to getWebSite", e);
		}
		return result;
	}

	@Override
	public List<FilterEntry> getSearchEngineData() throws TException {
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		try{
			result = parseData(filterDataService.getRefSEData());
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return result;
	}

	@Override
	public List<SEFilterEntry> getSiteSEAndType(int webId) throws TException {
		List<SEFilterEntry> result = new ArrayList<SEFilterEntry>();
		try{
			Map<Integer, String> seIdMap = searchDataService.getSiteSearchEngine();
			List<Integer> idList = new ArrayList<Integer>(seIdMap.keySet());
			Collections.sort(idList);
			for(int id: idList){
				Map<Integer, String> searchTypeMap = searchDataService.getSiteSearchType(id);
				if(searchTypeMap.size() > 0){
					List<Integer> searchTypeList = new ArrayList<Integer>(searchTypeMap.keySet());
					for(int searchType: searchTypeList){
						String name = seIdMap.get(id) + "-" + searchTypeMap.get(searchType);
						SEFilterEntry entry = new SEFilterEntry(name, id);
						entry.setSearchType(searchType);
						result.add(entry);
					}
				} else {
					result.add(new SEFilterEntry(seIdMap.get(id), id));
				}
			}
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return result;
	}

	@Override
	public List<FilterEntry> getSearchCondition(int seId, int seType)
			throws TException {
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		try{
			List<SiteSearchCondition> conlist = searchDataService.getSearchConditionList(seId, seType == 0? null: seType);
			Collections.sort(conlist, new Comparator<SiteSearchCondition>(){
				@Override
				public int compare(SiteSearchCondition o1, SiteSearchCondition o2) {
					return o1.getSortedNum() - o2.getSortedNum();
				}
			});
			for(SiteSearchCondition siteSeCon: conlist){
				result.add(new FilterEntry(siteSeCon.getName(), siteSeCon.getSeConType()));
			}
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return result;
	}

	@Override
	public List<FilterEntry> getUserType(int webId) throws TException {
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		try{
			result = parseData(webSiteDataService.getUserType(webId));
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return result;
	}

	@Override
	public List<FilterEntry> getLoginUserType(int webId) throws TException {
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		try{
			result = parseData(webSiteDataService.getLoginUserType(webId));
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return result;
	}


	@Override
	public List<FilterEntry> getVisitType(int webId) throws TException {
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		try{
			result = parseData(webSiteDataService.getVisitType(webId));
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return result;
	}
	
	@Override
	public int getVisitTypeOfSearch(int webId, int seId, int searchType)
			throws TException {
		int visitType = -1;
		try{
			Integer searchTypeTmp = null;
			if(searchType > 0)
				searchTypeTmp = searchType;
			String searchEngine = searchDataService.getSiteSearchEngineName(seId);
			visitType = webSiteDataService.getVisitTypeOfSearch(webId, searchEngine, searchTypeTmp);
		} catch(Exception e){
			logger.error("error to getSearchEngineData", e);
		}
		return visitType;
	}

	private List<FilterEntry> parseData(Map<Integer, String> idMap){
		List<FilterEntry> result = new ArrayList<FilterEntry>();
		List<Integer> idList = new ArrayList<Integer>(idMap.keySet());
		Collections.sort(idList);
		for(int id: idList){
			result.add(new FilterEntry(idMap.get(id), id));
		}
		return result;
	}

	public static void main(String[] args) throws TException {
		DataServiceHandler handler = new DataServiceHandler();
//		System.out.println(handler.getUserType(1));
//		System.out.println(handler.getSiteSEAndType(1));
		
		System.out.println(handler.getSearchCondition(2, 0));
	}
}
