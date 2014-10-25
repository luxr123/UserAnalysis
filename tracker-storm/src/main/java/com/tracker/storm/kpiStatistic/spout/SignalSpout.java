package com.tracker.storm.kpiStatistic.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.DateUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * 文件名：SignalSpout
 * 创建人：jason.hua
 * 创建日期：2014-10-11 下午3:23:15
 * 功能：每分钟定时发送一次会话清理信号，清除已经结束的会话。
 *
 */
public class SignalSpout extends  BaseRichSpout {
	private static final long serialVersionUID = 5449633203392021159L;
	private static Logger logger = LoggerFactory.getLogger(SignalSpout.class);
	private SpoutOutputCollector collector; //用于发送数据
	
	/**
	 * 清理信号streamId名
	 */
	public static final String STREAMID = "cleanTimeOutStream";
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			//计算当天日期
			long curTime = System.currentTimeMillis();
			String date = DateUtils.getDay(curTime);
			
			//发送数据，包括当天日期，以及目前毫秒级时间
			collector.emit(STREAMID,new Values(date, curTime));
			
			 //睡眠1分钟
			Thread.sleep(1000 * 60);
		} catch (InterruptedException e) {
			logger.error("error to sleep 1 minute", e);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(STREAMID,new Fields(FIELDS.date.toString(), FIELDS.curTime.toString()));
	}
	
	/**
	 * 
	 * 文件名：FIELDS
	 * 创建人：jason.hua
	 * 创建日期：2014-10-14 上午10:54:53
	 * 功能描述：用于声明发送的字段名，便于读取
	 */
	public static enum FIELDS{
		date, 	//日期YYYYMMDD 
		curTime //时间(毫秒级)
	}

}
