package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tracker.db.dao.kpi.SummableKpiDao;
import com.tracker.db.dao.kpi.SummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.SearchSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.service.entity.SummabkeKpiType;

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
			String kpiType = input.getStringByField(UserSessionBolt.FIELDS.kpiType.toString());
			String kpiKey = input.getStringByField(UserSessionBolt.FIELDS.kpiSign.toString());
			Object kpiObj = input.getValueByField(UserSessionBolt.FIELDS.kpiObj.toString());
			
			if(kpiType == null || kpiKey == null || kpiObj == null){
				LOG.warn(input.getSourceStreamId() + " => kpiType=" + kpiType + ", kpiKey=" + kpiKey + ",kpiObj=" + (kpiObj == null?"null":"not null"));
				return;
			}
			
			if(kpiType.equals(SummabkeKpiType.PAGE_KPI.toString())){
				summableKpiDao.updatePageKpi((Map<String, PageSummableKpi>)kpiObj);
			} else if(kpiType.equals(SummabkeKpiType.SEARCH_KPI.toString())){
				summableKpiDao.updateSearchKpi((Map<String, SearchSummableKpi>)kpiObj);
			} else if(kpiType.equals(SummabkeKpiType.WEBSITE_KPI.toString())){
				summableKpiDao.updateWebSiteKpi((Map<String, WebSiteSummableKpi>)kpiObj);
			}
		} catch(Exception e){
			LOG.error("error to DataUpdateBolt, input:" + input, e);
		} 
	}
}
