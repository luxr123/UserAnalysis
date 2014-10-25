package com.tracker.storm.common.basebolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
/**
 * 
 * 文件名：BaseBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:45:58
 * 功能描述：bolt的基本封装.
 *
 */
public class BaseBolt implements IRichBolt {
	protected TopologyContext m_context;
	protected OutputCollector m_collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		m_context = context;
		m_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
