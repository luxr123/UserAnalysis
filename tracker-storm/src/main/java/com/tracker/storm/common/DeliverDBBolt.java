package com.tracker.storm.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.storm.common.basebolt.AccessHbaseBolt;
/**
 * 
 * 文件名：DeliverDBBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:41:31
 * 功能描述：功能与DeliverBoltf一样,另外提供存取hbase的功能.
 * 需要一hbase保持长连接时,使用这个bolt.
 *
 */
public abstract class DeliverDBBolt extends AccessHbaseBolt {
	private static Logger logger = LoggerFactory.getLogger(DeliverDBBolt.class);
	public static final int INITBUFFSIZE_DEFAULT = 100;
	protected String  m_rowKey = null;
	protected Map<String,List<String>> m_outputStream;
	protected List<Map<String,Object>> m_inputBuff ;
	protected  int m_streamNum;
	protected String STREAMID;
	protected int m_buffSize ;
	public DeliverDBBolt(){
		this(INITBUFFSIZE_DEFAULT,null,null,null);
	}
	
	public DeliverDBBolt(String streamId){
		this(INITBUFFSIZE_DEFAULT,streamId,null,null);
	}
	
	public DeliverDBBolt(String streamId,String tableName){
		this(INITBUFFSIZE_DEFAULT,streamId,tableName,null);
	}
	
	public DeliverDBBolt(int initBuffSize,String streamId,String tableName,String zookeeper) {
		// TODO Auto-generated constructor stub
		super(tableName,zookeeper);
		m_inputBuff = new ArrayList<Map<String,Object>>(initBuffSize);
		m_outputStream =  new HashMap<String,List<String>>();
		m_buffSize = initBuffSize; 
		m_rowKey = null;
		STREAMID = streamId;
	}
	
	public abstract  List<String> getInputFields();
	
	public String addStream(List<String> strList){
		return addStream(STREAMID + m_streamNum++,strList);
	}
	
	public String addStream(String streamId, List<String> strList){
		if(streamId != null && strList != null){
			ArrayList<String> list = new ArrayList<String>(strList);
			if(!list.contains("mainkey"))
				list.add("mainkey");
			if(!list.contains(UserVisitLogFields.FIELDS.serverLogTime.toString()))
				list.add(UserVisitLogFields.FIELDS.serverLogTime.toString());
			m_outputStream.put(streamId, list);
		}
		return streamId;
	}
	
	public void setRowKey(String rowKey){
		m_rowKey = rowKey;
	}
	
	protected void emitValues(Tuple input){
		emitValues(null,null,input);
	}
	
	protected void emitValues(String mainKey,Tuple input){
		int pos;
		if(m_inputBuff.size() != 0)
			pos= m_inputBuff.size() - 1;
		else
			return;
		String serverLogTime = UserVisitLogFields.FIELDS.serverLogTime.toString();
		//add default infomation
		if( mainKey != null && mainKey != "")
			m_inputBuff.get(pos).put("mainkey", mainKey);
		if(!m_inputBuff.get(pos).containsKey(serverLogTime))
			m_inputBuff.get(pos).put(serverLogTime,String.valueOf(new Long(System.currentTimeMillis())));
//		else{
//			Object logTime = m_inputBuff.get(pos).get(serverLogTime);
//			if(String.class.isInstance(logTime)){
//				m_inputBuff.get(pos).put(serverLogTime, new Long((String)logTime));
//			}
//		}
		for (Entry<String, List<String>> entry: m_outputStream.entrySet()) {
			Values values = new Values();
			for(String str : entry.getValue()){
				if(m_inputBuff.get(pos).containsKey(str))
					values.add(m_inputBuff.get(pos).get(str));
				else
					values.add("");
			}
			try{
				m_collector.emit(entry.getKey(),input, values);
			}
			catch(Exception  e){
				logger.error("emit values error");
			}
		}
	}
	
	protected void emitValues(String mainKey,Long timeStamp,Tuple input){
		int pos;
		if(m_inputBuff.size() != 0)
			pos= m_inputBuff.size() - 1;
		else
			return;
		//add default infomation
		String serverLogTime = UserVisitLogFields.FIELDS.serverLogTime.toString();
		if( mainKey != null && mainKey != "")
			m_inputBuff.get(pos).put("mainkey", mainKey);
		if(timeStamp != null)
			m_inputBuff.get(pos).put(serverLogTime, timeStamp);
		for (Entry<String, List<String>> entry: m_outputStream.entrySet()) {
			Values values = new Values();
			for(String str : entry.getValue()){
				if(m_inputBuff.get(pos).containsKey(str))
					values.add(m_inputBuff.get(pos).get(str));
				else
					values.add("");
			}
			try{
				m_collector.emit(entry.getKey(),input, values);
			}
			catch(Exception  e){
				logger.error("emit values error");
			}
		}
		if(m_inputBuff.size() >= m_buffSize){
			m_inputBuff.clear();
		}
	}
	
	public void fillEmitField(Tuple input){
		Map<String,Object> item = new HashMap<String, Object>();
		for (Entry<String, List<String>> entry: m_outputStream.entrySet()) {
			for(String str : entry.getValue()){
				try{
					item.put(str,input.getValueByField(str));
				}catch(Exception e){
					item.put(str, "");
				}
			}
		}
		m_inputBuff.add(item);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		m_context = context;
		m_collector = collector;
		if(m_tableName != null)
			initTable();
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		for (Entry<String, List<String>> entry: m_outputStream.entrySet()) {
			declarer.declareStream(entry.getKey(), new Fields(entry.getValue()));
		}
	}
}
