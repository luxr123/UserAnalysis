package com.tracker.storm.kpiStatistic.service.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.tracker.common.utils.NumericUtil;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.db.util.RowUtil;

public class SummableKpiEntity implements Serializable{
	private static final long serialVersionUID = 8015707179268459162L;
	private Map<String, Map<String, WebSiteSummableKpi>> webSiteKpiMap = null;
	private Map<String, Map<String, PageSummableKpi>> pageKpiMap = null;
	private Map<String, Map<String, SearchSummableKpi>> searchKpiMap = null;

	public void addWebSiteKpi(String key, WebSiteSummableKpi kpi){
		if(webSiteKpiMap == null)
			webSiteKpiMap = new HashMap<String, Map<String, WebSiteSummableKpi>>();
		
		String sign = RowUtil.getRowField(key, WebSiteSummableKpi.SIGN_INDEX);
		if(sign == null)
			return;
		Map<String, WebSiteSummableKpi> kpiMap = webSiteKpiMap.get(sign);
		if(kpiMap == null){
			kpiMap = new HashMap<String, WebSiteSummableKpi>();
			webSiteKpiMap.put(sign, kpiMap);
		}
		
		WebSiteSummableKpi kpiResult = kpiMap.get(key);
		if(kpiResult == null){
			kpiResult = new WebSiteSummableKpi();
			kpiMap.put(key, kpiResult);
		}
		kpiResult.setPv(NumericUtil.addValue(kpiResult.getPv(), kpi.getPv()));
		kpiResult.setVisitTimes(NumericUtil.addValue(kpiResult.getVisitTimes(), kpi.getVisitTimes()));
		kpiResult.setTotalVisitTime(NumericUtil.addValue(kpiResult.getTotalVisitTime(), kpi.getTotalVisitTime()));
		kpiResult.setTotalJumpCount(NumericUtil.addValue(kpiResult.getTotalJumpCount(), kpi.getTotalJumpCount()));
		kpiResult.setTotalVisitPage(NumericUtil.addValue(kpiResult.getTotalVisitPage(), kpi.getTotalVisitPage()));
	}

	public void addPageKpi(String key, PageSummableKpi kpi){
		if(pageKpiMap == null)
			pageKpiMap = new HashMap<String, Map<String, PageSummableKpi>>();
		
		String sign = RowUtil.getRowField(key, PageSummableKpi.SIGN_INDEX);
		if(sign == null)
			return;
		Map<String, PageSummableKpi> kpiMap = pageKpiMap.get(sign);
		if(kpiMap == null){
			kpiMap = new HashMap<String, PageSummableKpi>();
			pageKpiMap.put(sign, kpiMap);
		}
		
		PageSummableKpi kpiResult = kpiMap.get(key);
		if(kpiResult == null){
			kpiResult = new PageSummableKpi();
			kpiMap.put(key, kpiResult);
		}
		kpiResult.setPv(NumericUtil.addValue(kpiResult.getPv(), kpi.getPv()));
		kpiResult.setEntryPageCount(NumericUtil.addValue(kpiResult.getEntryPageCount(), kpi.getEntryPageCount()));
		kpiResult.setNextPageCount(NumericUtil.addValue(kpiResult.getNextPageCount(), kpi.getNextPageCount()));
		kpiResult.setOutPageCount(NumericUtil.addValue(kpiResult.getOutPageCount(), kpi.getOutPageCount()));
		kpiResult.setStayTime(NumericUtil.addValue(kpiResult.getStayTime(), kpi.getStayTime()));
	}
	
	public void addSearchKpi(String key, SearchSummableKpi kpi){
		if(searchKpiMap == null)
			searchKpiMap = new HashMap<String, Map<String, SearchSummableKpi>>();
		
		String sign = RowUtil.getRowField(key, SearchSummableKpi.SIGN_INDEX);
		if(sign == null)
			return;
		Map<String, SearchSummableKpi> kpiMap = searchKpiMap.get(sign);
		if(kpiMap == null){
			kpiMap = new HashMap<String, SearchSummableKpi>();
			searchKpiMap.put(sign, kpiMap);
		}
		
		SearchSummableKpi kpiResult = kpiMap.get(key);
		if(kpiResult == null){
			kpiResult = new SearchSummableKpi();
			kpiMap.put(key, kpiResult);
		}
		kpiResult.setPv(NumericUtil.addValue(kpiResult.getPv(), kpi.getPv()));
		kpiResult.setTotalCost(NumericUtil.addValue(kpiResult.getTotalCost(), kpi.getTotalCost()));
	}
	

	public void mergeEntity(SummableKpiEntity kpiEntity){
		if(kpiEntity == null)
			return;
		if(kpiEntity.getWebSiteKpiMap() != null){
			for(String sign: kpiEntity.getWebSiteKpiMap().keySet()){
				Map<String, WebSiteSummableKpi> kpiMap = kpiEntity.getWebSiteKpiMap().get(sign);
				for(String key: kpiMap.keySet()){
					this.addWebSiteKpi(key, kpiMap.get(key));
				}
			}
		}
		
		if(kpiEntity.getPageKpiMap() != null){
			for(String sign: kpiEntity.getPageKpiMap().keySet()){
				Map<String, PageSummableKpi> kpiMap = kpiEntity.getPageKpiMap().get(sign);
				for(String key: kpiMap.keySet()){
					this.addPageKpi(key, kpiMap.get(key));
				}
			}
		}
		if(kpiEntity.getSearchKpiMap() != null){
			for(String sign: kpiEntity.getSearchKpiMap().keySet()){
				Map<String, SearchSummableKpi> kpiMap = kpiEntity.getSearchKpiMap().get(sign);
				for(String key: kpiMap.keySet()){
					this.addSearchKpi(key, kpiMap.get(key));
				}
			}
		}
	}
	
	public Map<String, Map<String, WebSiteSummableKpi>> getWebSiteKpiMap() {
		return webSiteKpiMap;
	}

	public Map<String, Map<String, PageSummableKpi>> getPageKpiMap() {
		return pageKpiMap;
	}

	public Map<String, Map<String, SearchSummableKpi>> getSearchKpiMap() {
		return searchKpiMap;
	}

	
}
