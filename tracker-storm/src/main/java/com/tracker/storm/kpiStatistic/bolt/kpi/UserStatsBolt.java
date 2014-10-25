package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.INDEX_FIELDS;
import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.service.entity.SummabkeKpiType;
import com.tracker.storm.kpiStatistic.service.entity.UserStatsEntity;

/**
 * 
 * 文件名：DataUpdateBolt
 * 创建人：jason.hua
 * 创建日期：2014-10-15 下午3:50:51
 * 功能描述： 更新可累加kpi值
 *
 */
public class UserStatsBolt extends BaseBolt{
	private static final long serialVersionUID = 2840879850884372356L;
	private static Logger LOG = LoggerFactory.getLogger(UserStatsBolt.class);
	
	private StormConfig config;//配置对象
	private HbaseCRUD cookieIndexTable;
	private HbaseCRUD userIndexTable;

	/**
	 * SummableKpiBolt构造函数
	 */
	public UserStatsBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		cookieIndexTable = new HbaseCRUD("cookie_index", config.getZookeeper());
		userIndexTable = new HbaseCRUD("user_index", config.getZookeeper());
	}
 
	@Override
	public void execute(Tuple input) {
		try {
			Object statsEntityObj = input.getValueByField(UserSessionBolt.FIELDS.statsEntity.toString());
			if(statsEntityObj == null ){
				LOG.warn(input.getSourceStreamId() + " => statsEntityObj=NULL");
				return;
			}
			
			UserStatsEntity statsEntity = (UserStatsEntity)statsEntityObj;
			if(statsEntity.getRowKeyOfCookie() != null){
				HbaseParam param = new HbaseParam();
				param.setRowkey(statsEntity.getRowKeyOfCookie());
				param.addInc(UserVisitLogFields.Index_InfoFam, INDEX_FIELDS.visitTime.toString(), statsEntity.getStayTime());
				cookieIndexTable.batchWrite(param, null);
			}
			if(statsEntity.getRowKeyOfUser() != null){
				HbaseParam param = new HbaseParam();
				param.setRowkey(statsEntity.getRowKeyOfUser());
				param.addInc(UserVisitLogFields.Index_InfoFam, INDEX_FIELDS.visitTime.toString(), statsEntity.getStayTime());
				userIndexTable.batchWrite(param, null);
			}
		} catch(Exception e){
			LOG.error("error to DataUpdateBolt, input:" + input, e);
		} 
	}
}
