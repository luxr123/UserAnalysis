package com.tracker.storm.kpiStatistic.service.handler;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.HConnection;

import com.tracker.common.cache.batch.KVBatchHandler;
import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.INDEX_FIELDS;
import com.tracker.common.utils.NumericUtil;
import com.tracker.db.dao.HBaseDao;

public class UserStatsHandler  extends KVBatchHandler<String, Long>{
	private HBaseDao  indexTable;

	public UserStatsHandler(HConnection hbaseConnection, String indexTableName, int batchSize, int period){
		super(batchSize, period);
		indexTable = new HBaseDao(hbaseConnection, indexTableName);
	}
	
	
	@Override
	protected Long updateValue(Long firstVal, Long secondVal) {
		return NumericUtil.addValue(firstVal, secondVal);
	}

	@Override
	protected void flush(ConcurrentHashMap<String, Long> cacheMap) {
		indexTable.batchIncValues(UserVisitLogFields.Index_InfoFam.getBytes(), INDEX_FIELDS.visitTime.toString().getBytes(), cacheMap);
	}
}
