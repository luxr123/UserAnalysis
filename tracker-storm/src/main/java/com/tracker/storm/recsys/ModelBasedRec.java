package com.tracker.storm.recsys;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

import com.tracker.common.log.JobInfo;
import com.tracker.common.utils.DateUtils;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
import com.tracker.storm.common.StormConfig;

public class ModelBasedRec {
	private static final int MAXRECNUM = 10;
	private HbaseCRUD userInterTable;
	private HbaseCRUD jobSimTable;

	public ModelBasedRec(StormConfig config) {
		userInterTable = new HbaseCRUD("user_interest", config.getZookeeper());
		jobSimTable = new HbaseCRUD("job_sim", config.getZookeeper());
	}

	public Map<String, Float> getLogitScore(String userid, JobInfo job, Map<String, Float> weight) {
		Map<String, Float> retScore = new HashMap<String, Float>();
		float intercept = weight.get("intercept");
		float weight_F1 = weight.get("f1");
		float weight_F2 = weight.get("f2");
		float weight_F3 = weight.get("f3");

		HbaseParam param = new HbaseParam();
		String jobid = job.getJobId();
		String reg = "(.*-" + jobid + "|" + jobid + "-.*)-[0-1]\\.\\d+";
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(reg));
		param.addFilter(filter);
		HbaseResult hresult = jobSimTable.getRowKeys(param);
		Map<String, Float> f3 = new HashMap<String, Float>();
		for (String rowkey : hresult.getRowKeys()) {
			String[] values = rowkey.split("-");
			String jobId = values[0];
			jobId = jobId.equals(jobid) ? values[1] : jobId;
			f3.put(jobId, Float.valueOf(values[2]));
		}
		Map<Float, String> scores = new TreeMap<Float, String>().descendingMap();
		for (String key : f3.keySet()) {
			String rowkey = userInterTable.getRowKey(userid + "-" + key + "-[0-1]\\.\\d+");
			float value_F1 = Float.valueOf(rowkey.split("-")[2]);
			float value_F2 = (float) DateUtils.getDays(DateUtils.getToday(), job.getEndDate()).size()
					/ DateUtils.getDays(job.getBeginDate(), job.getEndDate()).size();
			float score = intercept + weight_F1 * value_F1 + weight_F2 * value_F2 + weight_F3 * f3.get(key);
			float logitScore = (float) (1.0 / (1 + Math.exp(-score)));
			if (logitScore >= weight.get("score_thd"))
				scores.put(logitScore, key);
		}
		for (Entry<Float, String> entry : scores.entrySet()) {
			if (retScore.size() > MAXRECNUM) break;
			retScore.put(entry.getValue(), entry.getKey());
		}
		return retScore;
	}
}
