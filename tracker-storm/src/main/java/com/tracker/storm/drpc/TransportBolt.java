package com.tracker.storm.drpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.drpc.groupstream.GroupStream;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * 文件名：TransportBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:59:57
 * 功能描述：针对不同请求,调用不同的类处理请求,将返回的结果集转发给SearchRealTimeStatistic
 * .
 *
 */

public class TransportBolt extends BaseBolt {
	
	public static final String STREAMID = "transportStream";
	protected String COMPENTID = "transportBolt";
	protected String m_broadCompent;
	protected List<Integer> m_tasksIds;
	protected Integer m_myTaskId;
	protected Map<String,GroupStream> m_transport;
	protected Map<String,List<String>> m_outputStream;
	protected Map<String,String> m_inputStream;
	private Integer m_emitCount;
	private Integer m_offset ;
	
	public TransportBolt(String compent){
		m_broadCompent = compent;
		m_transport = new HashMap<String, GroupStream>();
		m_outputStream = new HashMap<String, List<String>>();
		m_inputStream = new HashMap<String, String>();
		m_offset = 0;
	}
	/**
	 * 
	 * 
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//shuffle to next bolt
		String request = input.getString(0);
		String splits[] = request.split("-");
		m_tasksIds = m_context.getComponentTasks(m_broadCompent);
		if(m_transport.containsKey(splits[0])){
			m_emitCount = m_emitCount % 7;
			List<Object> tmp = m_transport.get(splits[0]).group(this, request);
			for(Object element : tmp){
				int randomNum = m_emitCount++%m_tasksIds.size();
				m_collector.emitDirect(m_tasksIds.get(randomNum),STREAMID,new Values(element,input.getValue(1),tmp.size()));
			}
		}else{
			int randomNum = m_emitCount++%m_tasksIds.size();
			m_collector.emitDirect(m_tasksIds.get(randomNum),STREAMID,new Values(input.getValue(0),input.getValue(1),0));
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		m_tasksIds = context.getComponentTasks(m_broadCompent);
		List<Integer> tmp = context.getComponentTasks(COMPENTID);
		if(tmp != null && tmp.size() != 0)
			m_myTaskId = tmp.get(0);
		m_emitCount = 0;
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream(STREAMID,true,new Fields("duplication","ids","totalcount"));
	}
	
	public String addSourceStream(String source,List<String> strList){
		String streamId = null;
		if(source != null && strList != null){
			streamId = STREAMID + (m_offset++);
			ArrayList<String> list = new ArrayList<String>(strList);
			m_outputStream.put(streamId, list);
			m_inputStream.put(source, streamId);
		}
		return streamId;
	}
	
	public void addTransport(String key,GroupStream process){
		m_transport.put(key, process);
	}
	
	public Integer getTransportSize(){
		return m_tasksIds.size();
	}
	
	public static String getStreamId(){
		return STREAMID;
	}
	
	public static String getCompentId(){
		return "transportBolt";
	}
}
