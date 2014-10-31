package com.tracker.storm.recsys;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.tracker.common.log.JobInfo;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
import com.tracker.storm.common.StormConfig;

public class CaseBasedRec {
	private static final int MAXUSERSIMNUM = 1000;
	private static final int MAXRECNUM = 10;
	private HbaseCRUD userSimCrud;
	private HbaseCRUD jobCaseCrud;

	public CaseBasedRec(StormConfig config) {
		userSimCrud = new HbaseCRUD("user_sim", config.getZookeeper());
		jobCaseCrud = new HbaseCRUD("job_case", config.getZookeeper());
	}

	public Map<String, Float> getBasedCaseRecs(String userid, JobInfo job, Map<String, Float> weight) {
		Map<String, Float> retScore = new HashMap<String, Float>();
		String reg = "(.*-" + userid + "|" + userid + "-.*)-[0-1]\\.\\d+";
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(reg));
		HbaseParam param = new HbaseParam();
		param.addFilter(filter);
		HbaseResult hresult = userSimCrud.getRowKeys(param);
		Map<Float, String> usersim = new TreeMap<Float, String>().descendingMap();
		for (String rowkey : hresult.getRowKeys()) {
			String[] values = rowkey.split("-");
			String userId = values[0];
			userId = userId.equals(userid) ? values[1] : userId;
			usersim.put(Float.valueOf(values[2]), userId);
		}
		Set<String> candidates = new HashSet<String>();
		for (Entry<Float, String> entry : usersim.entrySet()) {
			if (candidates.size() > MAXUSERSIMNUM) break;
			candidates.add(entry.getValue());
		}
		
		usersim.clear();
		for(String candi : candidates){
			Map<String, Float> scoreMap = jobCaseCrud.getUerJobScoreDesc(userid, candi);
			for(Entry<String, Float> entry:scoreMap.entrySet())
				usersim.put(entry.getValue(), entry.getKey());
		}
		for (Entry<Float, String> entry : usersim.entrySet()) {
			if (retScore.size() > MAXRECNUM) break;
			retScore.put(entry.getValue(), entry.getKey());
		}

		return retScore;
	}
}
