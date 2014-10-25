package com.tracker.storm.kpiStatistic.bolt.kpi;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tracker.db.dao.webstats.UserSessionDao;
import com.tracker.db.dao.webstats.UserSessionHBaseDaoImpl;
import com.tracker.db.dao.webstats.model.UserSessionData;
import com.tracker.db.util.RowUtil;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.kpiStatistic.spout.SignalSpout;

public class SessionTimeOutBolt extends BaseBolt{
	private static final long serialVersionUID = 2003045811626509470L;
	private static Logger LOG = LoggerFactory.getLogger(SessionTimeOutBolt.class);
	private UserSessionDao userSessionDao;
	private final int sessionGapTime = 30 * 60 * 1000; // 30分钟
	private final static int END_SESSION_COUNT = 100; //每次获取结束会话的数量
	private StormConfig config = null;

	//stream name
	public static String SESSION_TIME_OUT_STREAM = "sessionTimeOutStream";
	
	public SessionTimeOutBolt(StormConfig config) {
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		userSessionDao = new UserSessionHBaseDaoImpl(config.getHbaseConnection());
	}

	@Override
	public void execute(Tuple input) {
		try{
			String date = input.getStringByField(SignalSpout.FIELDS.date.toString());
			Long curTime = input.getLongByField(SignalSpout.FIELDS.curTime.toString());
			if(curTime == null){
				LOG.warn("curTime is null");
				return;
			}
			List<String> keys = userSessionDao.getEndSessionKeys(UserSessionData.generateRowPrefix(date), curTime, sessionGapTime, END_SESSION_COUNT);
			if(keys != null && keys.size() > 0){
				for(String key: keys){
					String cookieId = RowUtil.getRowField(key, UserSessionData.COOKIE_ID_INDEX);
					m_collector.emit(SESSION_TIME_OUT_STREAM, input, new Values(cookieId, curTime, key));
				}
			}
		} catch(Exception e){
			LOG.error("error to get timeout session, input:" + input, e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SESSION_TIME_OUT_STREAM, new Fields(FIELDS.cookieId.toString(), FIELDS.curTime.toString(), FIELDS.key.toString()));
	}
	
	public static enum FIELDS{
		cookieId, curTime, key
	}
}
