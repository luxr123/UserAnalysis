package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.service.entity.SummableKpiEntity;

/**
 * 
 * 文件名：SummableKpiBolt
 * 创建人：jason.hua
 * 创建日期：2014-10-15 下午3:50:51
 * 功能描述： 更新可累加kpi值
 *
 */
public class SummableKpiBolt extends BaseBolt{
	private static final long serialVersionUID = 2840879850884372356L;
	private static Logger LOG = LoggerFactory.getLogger(SummableKpiBolt.class);
	
	private StormConfig config;//配置对象
	private SummableKpiDao summableKpiDao;//可累加数据访问对象


	/**
	 * SummableKpiBolt构造函数
	 */
	public SummableKpiBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		summableKpiDao = new SummableKpiHBaseDaoImpl(config.getHbaseConnection());
	}
 
	@Override
	public void execute(Tuple input) {
		try {
			Object kpiObj = input.getValueByField(UserSessionBolt.FIELDS.kpiObj.toString());
			if(kpiObj == null){
				LOG.warn(input.getSourceStreamId() + " => kpiObj=" + (kpiObj == null?"null":"not null"));
				return;
			}
			
			SummableKpiEntity kpiEntity = (SummableKpiEntity)kpiObj;
			if(kpiEntity.getPageKpiMap() != null){
				summableKpiDao.updatePageKpi(kpiEntity.getPageKpiMap());
			}
			if(kpiEntity.getSearchKpiMap() != null){
				summableKpiDao.updateSearchKpi(kpiEntity.getSearchKpiMap());
			} 
			if(kpiEntity.getWebSiteKpiMap() != null){
				summableKpiDao.updateWebSiteKpi(kpiEntity.getWebSiteKpiMap());
			}	
		} catch(Exception e){
			LOG.error("error to DataUpdateBolt, input:" + input, e);
		} 
	}
}
