package com.tracker.storm.kpiStatistic.service.handler;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.cache.batch.KVBatchHandler;
import com.tracker.common.utils.NumericUtil;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;

public class WebSiteSummableKpiHandler extends KVBatchHandler<String, WebSiteSummableKpi>{
	
	private SummableKpiDao summableKpiDao;//可累加数据访问对象

	public WebSiteSummableKpiHandler(HConnection hconnection, int batchSize, int period){
		super(batchSize, period);
		summableKpiDao = new SummableKpiHBaseDaoImpl(hconnection);
	}
	
	@Override
	protected WebSiteSummableKpi updateValue(WebSiteSummableKpi firstVal,
			WebSiteSummableKpi secondVal) {
		WebSiteSummableKpi kpiResult = new WebSiteSummableKpi();
		kpiResult.setPv(NumericUtil.addValue(firstVal.getPv(), secondVal.getPv()));
		kpiResult.setVisitTimes(NumericUtil.addValue(firstVal.getVisitTimes(), secondVal.getVisitTimes()));
		kpiResult.setTotalVisitTime(NumericUtil.addValue(firstVal.getTotalVisitTime(), secondVal.getTotalVisitTime()));
		kpiResult.setTotalJumpCount(NumericUtil.addValue(firstVal.getTotalJumpCount(), secondVal.getTotalJumpCount()));
		kpiResult.setTotalVisitPage(NumericUtil.addValue(firstVal.getTotalVisitPage(), secondVal.getTotalVisitPage()));
		return kpiResult;
	}

	@Override
	protected void flush(ConcurrentHashMap<String, WebSiteSummableKpi> cacheMap) {
		summableKpiDao.updateWebSiteKpi(cacheMap);
	}
}
