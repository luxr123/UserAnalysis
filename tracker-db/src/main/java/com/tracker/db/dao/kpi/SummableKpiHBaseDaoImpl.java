package com.tracker.db.dao.kpi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.utils.DoublePriorityQueue;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.db.simplehbase.request.PutRequest;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;

/**
 * 基于hbase的网站可累加指标统计
 * @author jason.hua
 *
 */
public class SummableKpiHBaseDaoImpl implements SummableKpiDao{
	private HBaseDao summableKpiTable;
	
	public SummableKpiHBaseDaoImpl(HConnection hbaseConnection) {
		summableKpiTable = new HBaseDao(hbaseConnection, WebSiteSummableKpi.class);
	}

	@Override
	public void updateWebSiteKpi(String row, WebSiteSummableKpi kpi) {
		Map<String, WebSiteSummableKpi> kpiMap = new HashMap<String, WebSiteSummableKpi>();
		kpiMap.put(row, kpi);
		updateWebSiteKpi(kpiMap);
	}
	
	@Override
	public void updateWebSiteKpi(Map<String, WebSiteSummableKpi> kpiMap) {
		List<PutRequest<WebSiteSummableKpi>> putRequestList = new ArrayList<PutRequest<WebSiteSummableKpi>>();
		for(String key: kpiMap.keySet()){
			putRequestList.add(new PutRequest<WebSiteSummableKpi>(key, kpiMap.get(key)));
		}
		summableKpiTable.putObjectList(putRequestList);
	}

	@Override
	public void updatePageKpi(String row, PageSummableKpi kpi) {
		Map<String, PageSummableKpi> kpiMap = new HashMap<String, PageSummableKpi>();
		kpiMap.put(row, kpi);
		updatePageKpi(kpiMap);
	}
	
	@Override
	public void updatePageKpi(Map<String, PageSummableKpi> kpiMap) {
		List<PutRequest<PageSummableKpi>> putRequestList = new ArrayList<PutRequest<PageSummableKpi>>();
		for(String key: kpiMap.keySet()){
			putRequestList.add(new PutRequest<PageSummableKpi>(key, kpiMap.get(key)));
		}
		summableKpiTable.putObjectList(putRequestList);
	}


	@Override
	public void updateSearchKpi(String row, SearchSummableKpi kpi) {
		Map<String, SearchSummableKpi> kpiMap = new HashMap<String, SearchSummableKpi>();
		kpiMap.put(row, kpi);
		updateSearchKpi(kpiMap);
	}

	@Override
	public void updateSearchKpi(Map<String, SearchSummableKpi> kpiMap) {
		List<PutRequest<SearchSummableKpi>> putRequestList = new ArrayList<PutRequest<SearchSummableKpi>>();
		for(String key: kpiMap.keySet()){
			putRequestList.add(new PutRequest<SearchSummableKpi>(key, kpiMap.get(key)));
		}
		summableKpiTable.putObjectList(putRequestList);
	}

	@Override
	public Map<String, Long> getWebSitePVKpi(List<String> rowPrefixList, Integer fieldIndex) {
		QueryExtInfo<WebSiteSummableKpi> queryInfo = new QueryExtInfo<WebSiteSummableKpi>();
		queryInfo.addColumn(WebSiteSummableKpi.Columns.pv.toString());
		List<SimpleHbaseDOWithKeyResult<WebSiteSummableKpi>> list = summableKpiTable.findObjectListAndKeyByRowPrefixList(rowPrefixList, WebSiteSummableKpi.class, queryInfo);
		
		Map<String, Long> pvMap = new HashMap<String, Long>();
		for(SimpleHbaseDOWithKeyResult<WebSiteSummableKpi> rowObj: list){
			String field = RowUtil.getRowField(rowObj.getRowKey(), fieldIndex);
			if(field == null)
				continue;
			Long pv = rowObj.getT().getPv();
			if(pv == null)
				continue;
			if(pvMap.containsKey(field)){
				pvMap.put(field, pv + pvMap.get(field));
			} else {
				pvMap.put(field, pv);
			}
		}
		return pvMap;
	}

	@Override
	public Map<String, WebSiteSummableKpi> getWebSiteKpi(List<String> rowPrefixList, Integer fieldIndex) {
		Map<String, WebSiteSummableKpi> result = new HashMap<String, WebSiteSummableKpi>();
		
		List<SimpleHbaseDOWithKeyResult<WebSiteSummableKpi>> list = summableKpiTable.findObjectListAndKeyByRowPrefixList(rowPrefixList, WebSiteSummableKpi.class, null);
		
		for(SimpleHbaseDOWithKeyResult<WebSiteSummableKpi> rowObj: list){
			String field = RowUtil.getRowField(rowObj.getRowKey(), fieldIndex);
			if(field == null)
				continue;
			WebSiteSummableKpi summableKpi = rowObj.getT();
			Long pv = rowObj.getT().getPv();
			if(pv == null)
				continue;
			Long visitTimes = summableKpi.getVisitTimes();
			Long totalVisitTime = summableKpi.getTotalVisitTime();
			Long totalJumpCount = summableKpi.getTotalJumpCount();
			Long totalVisitPage = summableKpi.getTotalVisitPage();
			WebSiteSummableKpi kpiResult = result.get(field);
			if(kpiResult == null){
				kpiResult = new WebSiteSummableKpi();
				result.put(field, kpiResult);
			}
			kpiResult.setPv((kpiResult.getPv() == null ?0: kpiResult.getPv()) + (pv == null? 0: pv));
			kpiResult.setVisitTimes((kpiResult.getVisitTimes() == null? 0: kpiResult.getVisitTimes()) + (visitTimes == null? 0: visitTimes));
			kpiResult.setTotalVisitTime((kpiResult.getTotalVisitTime() == null? 0: kpiResult.getTotalVisitTime()) + (totalVisitTime == null? 0: totalVisitTime));
			kpiResult.setTotalJumpCount((kpiResult.getTotalJumpCount() == null? 0: kpiResult.getTotalJumpCount()) + (totalJumpCount == null? 0: totalJumpCount));
			kpiResult.setTotalVisitPage((kpiResult.getTotalVisitPage() == null? 0: kpiResult.getTotalVisitPage()) + (totalVisitPage == null? 0: totalVisitPage));
		}
		return result;
	}

	@Override
	public Map<String, WebSiteSummableKpi> getWebSiteTopKpi(List<String> rowPrefixList, Integer fieldIndex, int topCount) {
		List<SimpleHbaseDOWithKeyResult<WebSiteSummableKpi>> list = summableKpiTable.findObjectListAndKeyByRowPrefixList(rowPrefixList, WebSiteSummableKpi.class, null);
		Map<String, WebSiteSummableKpi> resultMap = new HashMap<String, WebSiteSummableKpi>();
		for(SimpleHbaseDOWithKeyResult<WebSiteSummableKpi> rowObj: list){
			String field = RowUtil.getRowField(rowObj.getRowKey(), fieldIndex);
			if(field == null)
				continue;
			WebSiteSummableKpi summableKpi = rowObj.getT();
			Long pv = rowObj.getT().getPv();
			Long visitTimes = summableKpi.getVisitTimes();
			Long totalVisitTime = summableKpi.getTotalVisitTime();
			Long totalJumpCount = summableKpi.getTotalJumpCount();
			Long totalVisitPage = summableKpi.getTotalVisitPage();
			WebSiteSummableKpi kpiResult = resultMap.get(field);
			if(kpiResult == null){
				kpiResult = new WebSiteSummableKpi();
				resultMap.put(field, kpiResult);
			}
			kpiResult.setPv((kpiResult.getPv() == null ?0: kpiResult.getPv()) + (pv == null? 0: pv));
			kpiResult.setVisitTimes((kpiResult.getVisitTimes() == null? 0: kpiResult.getVisitTimes()) + (visitTimes == null? 0: visitTimes));
			kpiResult.setTotalVisitTime((kpiResult.getTotalVisitTime() == null? 0: kpiResult.getTotalVisitTime()) + (totalVisitTime == null? 0: totalVisitTime));
			kpiResult.setTotalJumpCount((kpiResult.getTotalJumpCount() == null? 0: kpiResult.getTotalJumpCount()) + (totalJumpCount == null? 0: totalJumpCount));
			kpiResult.setTotalVisitPage((kpiResult.getTotalVisitPage() == null? 0: kpiResult.getTotalVisitPage()) + (totalVisitPage == null? 0: totalVisitPage));
		}
		
		DoublePriorityQueue<String> queue = new DoublePriorityQueue<String>(topCount);
		for(String key: resultMap.keySet()){
			WebSiteSummableKpi kpiResult = resultMap.get(key);
			queue.add(kpiResult.getPv(), key);
		}
		List<String> queueList = queue.values();
		Map<String, WebSiteSummableKpi> result = new HashMap<String, WebSiteSummableKpi>();
		for(int i = 0; i < queueList.size(); i++){
			result.put(queueList.get(i), resultMap.get(queueList.get(i)));
		}
		return result;
	}

	@Override
	public Map<String, PageSummableKpi> getWebSitePageKpi(List<String> rowPrefixList, Integer fieldIndex) {
		Map<String, PageSummableKpi> result = new HashMap<String, PageSummableKpi>();
		List<SimpleHbaseDOWithKeyResult<PageSummableKpi>> list = summableKpiTable.findObjectListAndKeyByRowPrefixList(rowPrefixList, PageSummableKpi.class, null);
		
		for(SimpleHbaseDOWithKeyResult<PageSummableKpi> rowObj: list){
			String field = RowUtil.getRowField(rowObj.getRowKey(), fieldIndex);
			if(field == null)
				continue;
			PageSummableKpi summableKpi = rowObj.getT();
			Long pv = summableKpi.getPv();
			Long entryPageCount = summableKpi.getEntryPageCount();
			Long nextPageCount = summableKpi.getNextPageCount();
			Long outPageCount = summableKpi.getOutPageCount();
			Long stayTime = summableKpi.getStayTime();
			PageSummableKpi kpiResult = result.get(field);
			if(kpiResult == null){
				kpiResult = new PageSummableKpi();
				result.put(field, kpiResult);
			}
			
			kpiResult.setPv((kpiResult.getPv() == null?0:kpiResult.getPv()) + (pv == null? 0: pv));
			kpiResult.setEntryPageCount((kpiResult.getEntryPageCount() == null? 0: kpiResult.getEntryPageCount()) + (entryPageCount == null? 0: entryPageCount));
			kpiResult.setNextPageCount((kpiResult.getNextPageCount() == null? 0: kpiResult.getNextPageCount()) + (nextPageCount == null? 0: nextPageCount));
			kpiResult.setOutPageCount((kpiResult.getOutPageCount() == null? 0: kpiResult.getOutPageCount()) + (outPageCount == null? 0: outPageCount));
			kpiResult.setStayTime((kpiResult.getStayTime() == null? 0: kpiResult.getStayTime()) + (stayTime == null? 0: stayTime));
		}
		return result;
	}

	@Override
	public Map<String, SearchSummableKpi> getSearchKpi(String rowPrefix, Integer fieldIndex) {
		Map<String, SearchSummableKpi> result = new HashMap<String, SearchSummableKpi>();
		
		List<SimpleHbaseDOWithKeyResult<SearchSummableKpi>> list = summableKpiTable.findObjectListAndKeyByRowPrefix(rowPrefix, SearchSummableKpi.class, null);
		
		for(SimpleHbaseDOWithKeyResult<SearchSummableKpi> rowObj: list){
			String field = RowUtil.getRowField(rowObj.getRowKey(), fieldIndex);
			if(field == null)
				continue;
			SearchSummableKpi summableKpi = rowObj.getT();
			Long pv = summableKpi.getPv();
			Long totalCost = summableKpi.getTotalCost();
			SearchSummableKpi kpiResult = result.get(field);
			if(kpiResult == null){
				kpiResult = new SearchSummableKpi();
				result.put(field, kpiResult);
			}
			
			kpiResult.setPv((kpiResult.getPv() == null? 0: kpiResult.getPv()) + (pv == null? 0: pv));
			kpiResult.setTotalCost((kpiResult.getTotalCost() == null? 0: kpiResult.getTotalCost()) + (totalCost == null? 0: totalCost));
		}
		return result;
	}
}

