package com.tracker.storm.kpiStatistic.service.handler;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.cache.batch.KVBatchHandler;
import com.tracker.common.utils.NumericUtil;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.model.PageSummableKpi;

public class PageSummableKpiHandler extends KVBatchHandler<String, PageSummableKpi>{
	
	private SummableKpiDao summableKpiDao;//可累加数据访问对象

	public PageSummableKpiHandler(HConnection hbaseConnection, int batchSize, int period){
		super(batchSize, period);
		summableKpiDao = new SummableKpiHBaseDaoImpl(hbaseConnection);
	}
	
	@Override
	protected PageSummableKpi updateValue(PageSummableKpi firstVal, PageSummableKpi secondVal) {
		PageSummableKpi kpiResult = new PageSummableKpi();
		kpiResult.setPv(NumericUtil.addValue(firstVal.getPv(), secondVal.getPv()));
		kpiResult.setEntryPageCount(NumericUtil.addValue(firstVal.getEntryPageCount(), secondVal.getEntryPageCount()));
		kpiResult.setNextPageCount(NumericUtil.addValue(firstVal.getNextPageCount(), secondVal.getNextPageCount()));
		kpiResult.setOutPageCount(NumericUtil.addValue(firstVal.getOutPageCount(), secondVal.getOutPageCount()));
		kpiResult.setStayTime(NumericUtil.addValue(firstVal.getStayTime(), secondVal.getStayTime()));
		return kpiResult;
	}

	@Override
	protected void flush(ConcurrentHashMap<String, PageSummableKpi> cacheMap) {
		summableKpiDao.updatePageKpi(cacheMap);
	}
}
