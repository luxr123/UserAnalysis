package com.tracker.storm.kpiStatistic.service.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.tracker.common.utils.NumericUtil;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;

public class SummableKpiEntity implements Serializable{
	private static final long serialVersionUID = 1L;
	private Map<String, WebSiteSummableKpi> webSiteKpiMap = null;
	private Map<String, PageSummableKpi> pageKpiMap = null;
	private Map<String, SearchSummableKpi> searchKpiMap = null;

	public void addWebSiteKpi(String key, WebSiteSummableKpi kpi){
		if(webSiteKpiMap == null)
			webSiteKpiMap = new HashMap<String, WebSiteSummableKpi>();
		
		WebSiteSummableKpi kpiResult = null;
		if(webSiteKpiMap.containsKey(key)){
			kpiResult = webSiteKpiMap.get(key);
		} else {
			kpiResult = new WebSiteSummableKpi();
			webSiteKpiMap.put(key, kpiResult);
		}
		kpiResult.setPv(NumericUtil.addValue(kpiResult.getPv(), kpi.getPv()));
		kpiResult.setVisitTimes(NumericUtil.addValue(kpiResult.getVisitTimes(), kpi.getVisitTimes()));
		kpiResult.setTotalVisitTime(NumericUtil.addValue(kpiResult.getTotalVisitTime(), kpi.getTotalVisitTime()));
		kpiResult.setTotalJumpCount(NumericUtil.addValue(kpiResult.getTotalJumpCount(), kpi.getTotalJumpCount()));
		kpiResult.setTotalVisitPage(NumericUtil.addValue(kpiResult.getTotalVisitPage(), kpi.getTotalVisitPage()));
	}

	public void addPageKpi(String key, PageSummableKpi kpi){
		if(pageKpiMap == null)
			pageKpiMap = new HashMap<String, PageSummableKpi>();
		
		PageSummableKpi kpiResult = null;
		if(pageKpiMap.containsKey(key)){
			kpiResult = pageKpiMap.get(key);
		} else {
			kpiResult = new PageSummableKpi();
			pageKpiMap.put(key, kpiResult);
		}
		kpiResult.setPv(NumericUtil.addValue(kpiResult.getPv(), kpi.getPv()));
		kpiResult.setEntryPageCount(NumericUtil.addValue(kpiResult.getEntryPageCount(), kpi.getEntryPageCount()));
		kpiResult.setNextPageCount(NumericUtil.addValue(kpiResult.getNextPageCount(), kpi.getNextPageCount()));
		kpiResult.setOutPageCount(NumericUtil.addValue(kpiResult.getOutPageCount(), kpi.getOutPageCount()));
		kpiResult.setStayTime(NumericUtil.addValue(kpiResult.getStayTime(), kpi.getStayTime()));
	}
	
	public void addSearchKpi(String key, SearchSummableKpi kpi){
		if(searchKpiMap == null)
			searchKpiMap = new HashMap<String, SearchSummableKpi>();
		
		SearchSummableKpi kpiResult = null;
		if(searchKpiMap.containsKey(key)){
			kpiResult = searchKpiMap.get(key);
		} else {
			kpiResult = new SearchSummableKpi();
			searchKpiMap.put(key, kpiResult);
		}
		kpiResult.setPv(NumericUtil.addValue(kpiResult.getPv(), kpi.getPv()));
		kpiResult.setTotalCost(NumericUtil.addValue(kpiResult.getTotalCost(), kpi.getTotalCost()));
	}
	

	public void mergeEntity(SummableKpiEntity kpiEntity){
		if(kpiEntity == null)
			return;
		if(kpiEntity.getWebSiteKpiMap() != null){
			for(String key: kpiEntity.getWebSiteKpiMap().keySet()){
				this.addWebSiteKpi(key, kpiEntity.getWebSiteKpiMap().get(key));
			}
		}
		
		if(kpiEntity.getPageKpiMap() != null){
			for(String key: kpiEntity.getPageKpiMap().keySet()){
				this.addPageKpi(key, kpiEntity.getPageKpiMap().get(key));
			}
		}
		if(kpiEntity.getSearchKpiMap() != null){
			for(String key: kpiEntity.getSearchKpiMap().keySet()){
				this.addSearchKpi(key, kpiEntity.getSearchKpiMap().get(key));
			}
		}
	}
	
	public Map<String, WebSiteSummableKpi> getWebSiteKpiMap() {
		return webSiteKpiMap;
	}

	public Map<String, PageSummableKpi> getPageKpiMap() {
		return pageKpiMap;
	}

	public Map<String, SearchSummableKpi> getSearchKpiMap() {
		return searchKpiMap;
	}

	
}
