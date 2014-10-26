package com.tracker.storm.recsys;

import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.storm.common.StormConfig;

public class CaseBasedRec {
	private StormConfig config;//配置对象
	private HbaseCRUD cookieIndexTable;
	private HbaseCRUD userIndexTable;
}
