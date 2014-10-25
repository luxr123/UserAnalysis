package com.tracker.storm.kpiStatistic.bolt;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.model.SearchLog;
import com.tracker.db.dao.webstats.model.WebVisitLog;
import com.tracker.storm.common.DeliverBolt;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.data.DataService;
import com.tracker.storm.kpiStatistic.spout.ApacheLogSpout;

/**
 * 
 * 文件名：SaveApacheLogBolt
 * 创建人：jason.hua
 * 创建日期：2014-10-14 上午10:59:22
 * 功能描述：存储用户访问日志，包括浏览日志、搜索日志等。
 *
 */
public class SaveApacheLogBolt extends DeliverBolt {
	private static final long serialVersionUID = 5325351084891149930L;
	private static Logger LOG = LoggerFactory.getLogger(SaveApacheLogBolt.class);
	
	/**
	 * 数据访问对象
	 */
	private HBaseDao visitLogDao; //浏览日志接口对象，用于保存浏览日志
	private HBaseDao searchLogDao; //搜索日志接口对象, 用于保存搜索日志
	
	/**
	 * 其他配置
	 */
	private StormConfig config; //zookeeper地址
	private DataService dataService; //数据服务，用于获取访问类型visitType值
	private final String visitTypeName = "visit";//默认浏览访问类型名
	private Random random; //随机值对象，用于存储的时候，区分开同一访问时间下的访问日志信息
	
	public final static String LOG_STREAM = "saveLogStream";
	
	public SaveApacheLogBolt(StormConfig config) {
		super(LOG_STREAM);
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		visitLogDao = new HBaseDao(config.getHbaseConnection(), WebVisitLog.class);
		searchLogDao = new HBaseDao(config.getHbaseConnection(), SearchLog.class);
		dataService = new DataService(config.getHbaseConnection());
		random = new Random();
	}
	
	@Override
	public void execute(Tuple input) {
		try{
			String logType = input.getStringByField(ApacheLogSpout.FIELDS.logType.toString());
			Object logObj = input.getValueByField(ApacheLogSpout.FIELDS.apacheLogObj.toString());

			if(logType == null || logObj == null)
				return;
			
			//保存浏览记录
			if(ApachePVLog.APACHE_PV_LOG_TYPE.equalsIgnoreCase(logType)){
				WebVisitLog log = WebVisitLog.getLog((ApachePVLog)logObj);
				Integer visitType = dataService.getVisitType(Integer.parseInt(log.getWebId()), visitTypeName, null);
				if(visitType <= 0){
					LOG.warn("visitType is below and equal zero ,  webId:" + log.getWebId() + ", sign:" + visitTypeName);
					return;
				}
				log.setVisitType(visitType);
				String logRowKey = WebVisitLog.generateRowKey(log.getServerLogTime(), random.nextInt(10000), log.getCookieId()
						, log.getUserId(), log.getUserType(), log.getVisitType(), log.getWebId());
				
				visitLogDao.putObjectMV(logRowKey, log, log.getServerLogTime());
				//发送
				emitData(input, visitType, logRowKey, log.getWebId(), log.getServerLogTime()+ "",
						log.getUserType()+ "", log.getUserId(), log.getCookieId(), log.getIp());
			} 
			//保存搜索记录
			else if(ApacheSearchLog.APACHE_SEARCH_LOG_TYPE.equalsIgnoreCase(logType)){
				SearchLog searchLog = SearchLog.getLog((ApacheSearchLog)logObj);
				Integer visitType = dataService.getVisitType(Integer.parseInt(searchLog.getWebId()), searchLog.getCategory(), searchLog.getSearchType());
				if(visitType <= 0){
					LOG.warn("visitType is below and equal zero ,  webId:" + searchLog.getWebId() + ", sign:" + visitTypeName);
					return;
				}
				searchLog.setVisitType(visitType);
				String logRowKey = SearchLog.generateRowKey(searchLog.getServerLogTime(), random.nextInt(10000), searchLog.getCookieId()
						, searchLog.getUserId(), searchLog.getUserType(), searchLog.getVisitType(), searchLog.getWebId());
				searchLogDao.putObjectMV(logRowKey, searchLog, searchLog.getServerLogTime());

				//发送
				emitData(input, visitType, logRowKey, searchLog.getWebId(), searchLog.getServerLogTime() + "",
						searchLog.getUserType() + "", searchLog.getUserId(), searchLog.getCookieId(), searchLog.getIp());
			}
		}catch(Exception e){
			LOG.error("error to save visitLog, input:" + input,e);
		}
	}
	
	private void emitData(Tuple input, Integer visitType, String rowKey, String webId, String serverLogTime, String userType, String userId, 
			String cookieId, String ip){
		pushField(FIELDS.visitType.toString(), visitType);
		pushField(FIELDS.mainkey.toString(), rowKey);
		pushField(FIELDS.webId.toString(), webId);
		pushField(FIELDS.serverLogTime.toString(), serverLogTime);
		pushField(FIELDS.userType.toString(), userType);
		pushField(FIELDS.userId.toString(), userId);
		pushField(FIELDS.cookieId.toString(), cookieId);
		pushField(FIELDS.ip.toString(), ip);
		
		//发送
		emitValues(input);
	}
	
	public List<String> getInputFields(){
		return Lists.newArrayList(ApacheLogSpout.FIELDS.apacheLogObj.toString(), ApacheLogSpout.FIELDS.logType.toString(), ApacheLogSpout.FIELDS.cookieId.toString());
	}
	
	public static enum FIELDS{
		webId, visitType, serverLogTime, userType, mainkey, userId, cookieId, ip
	}
}
