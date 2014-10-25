package com.tracker.storm.kpiStatistic.service.handler;

import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.cache.batch.UniqueBatchHandler;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;

public class UnSummableKpiHandler extends UniqueBatchHandler<String>{
	
	private UnSummableKpiDao unSummableKpiDao = null;

	public UnSummableKpiHandler(HConnection hbaseConnection, int batchSize, int period){
		super(batchSize, period);
		unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(hbaseConnection, UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_DAY_TABLE);
	}

	@Override
	protected void flush(Set<String> batch) {
		unSummableKpiDao.updateUnSummableKpi(new ArrayList<String>(batch));
	}
}
