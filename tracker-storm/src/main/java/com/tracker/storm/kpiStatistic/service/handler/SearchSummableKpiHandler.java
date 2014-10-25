package com.tracker.storm.kpiStatistic.service.handler;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.cache.batch.KVBatchHandler;
import com.tracker.common.utils.NumericUtil;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;

public class SearchSummableKpiHandler extends KVBatchHandler<String, SearchSummableKpi>{
	
	private SummableKpiDao summableKpiDao;//可累加数据访问对象

	public SearchSummableKpiHandler(HConnection hbaseConnection, int batchSize, int period){
		super(batchSize, period);
		summableKpiDao = new SummableKpiHBaseDaoImpl(hbaseConnection);
	}
	
	@Override
	protected SearchSummableKpi updateValue(SearchSummableKpi firstVal, SearchSummableKpi secondVal) {
		SearchSummableKpi kpiResult = new SearchSummableKpi();
		kpiResult.setPv(NumericUtil.addValue(firstVal.getPv(), secondVal.getPv()));
		kpiResult.setTotalCost(NumericUtil.addValue(firstVal.getTotalCost(), secondVal.getTotalCost()));
		return kpiResult;
	}

	@Override
	protected void flush(ConcurrentHashMap<String, SearchSummableKpi> cacheMap) {
		summableKpiDao.updateSearchKpi(cacheMap);
	}
}
