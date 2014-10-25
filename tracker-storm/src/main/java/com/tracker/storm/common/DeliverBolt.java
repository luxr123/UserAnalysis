package com.tracker.storm.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.storm.common.basebolt.BaseBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * 文件名：DeliverBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:25:44
 * 功能描述：动态创建流的bolt.在原始的storm
 * api中,流的声明需要用户在上一级的bolt或者
 * spout(流的源头)的declareOutputFields函数
 * 中给出.这个类将流的声明放到了topology创建时,
 * 由流的消费bolt(不一定是DeliverBolt)调用源头
 * (DeliverBolt)的addStream函数声明流.另外,添加
 * emitValues函数替代emit函数,getInputFields()声明
 * 需要获取的字段.
 *
 */
public abstract class DeliverBolt extends BaseBolt{
	private static Logger logger = LoggerFactory.getLogger(DeliverBolt.class);
	public static final int INITBUFFSIZE_DEFAULT = 100;
	protected Map<String,List<String>> m_outputStream;
	protected List<Map<String,Object>> m_inputBuff ;
	protected  int m_streamNum;
	protected String STREAMID;
	protected int m_buffSize ;
	private boolean m_isMalloced;
	public DeliverBolt(String streamId){
		this(INITBUFFSIZE_DEFAULT,streamId);
	}
	public DeliverBolt(int initBuffSize,String streamId) {
		// TODO Auto-generated constructor stub
		m_inputBuff = new ArrayList<Map<String,Object>>(initBuffSize);
		m_outputStream =  new HashMap<String,List<String>>();
		m_buffSize = initBuffSize; 
		STREAMID = streamId;
		m_isMalloced = false;
	}
	
	public abstract  List<String> getInputFields();
	/**
	 * 
	 * 函数名：addStream
	 * 功能描述：缓存声明的流信息
	 * @param strList 流所需要的参数
	 * @return 流的名称
	 */
	public String addStream(List<String> strList){
		return addStream(STREAMID + m_streamNum++,strList);
	}
	/**
	 * 
	 * 函数名：addStream
	 * 功能描述：缓存声明的流信息
	 * @param streamId 显示设置流的名称
	 * @param strList  流所需要的参数
	 * @return 流的名称
	 */
	public String addStream(String streamId, List<String> strList){
		if(streamId != null && strList != null){
			ArrayList<String> list = new ArrayList<String>(strList);
			m_outputStream.put(streamId, list);
		}
		return streamId;
	}
	/**
	 * 
	 * 函数名：pushField
	 * 功能描述：对自身缓存字段集添加字段,如果未分配则显示构造一个字段集
	 * @param field key值
	 * @param value value值
	 */
	protected void pushField(String field,Object value){
		if(m_inputBuff.size() == 0 && m_isMalloced == false){
			m_inputBuff.add(new HashMap<String, Object>());
			m_isMalloced = true;
		}
		m_inputBuff.get(m_inputBuff.size()-1).put(field, value);
	}
	
	protected void emitValues(Tuple input){
		emitValues("",input);
	}
	/**
	 * 
	 * 函数名：emitValues
	 * 功能描述：从字段集中取出字段,发送缓存的流
	 * @param mainKey 用于标记同一信息解析所产生的流
	 * @param input 用于流的发射
	 */
	protected void emitValues(String mainKey,Tuple input){
		int pos;
		if(m_inputBuff.size() != 0)
			pos= m_inputBuff.size() - 1;
		else
			return;  
		//add default infomation
		if( mainKey != null && mainKey != "")
			m_inputBuff.get(pos).put("mainkey", mainKey);
		
		for (Entry<String, List<String>> entry: m_outputStream.entrySet()) {
			Values values = new Values();
			for(String str : entry.getValue()){
				if(m_inputBuff.get(pos).containsKey(str))
					values.add(m_inputBuff.get(pos).get(str));
				else
					values.add(null);
			}
			try{
				m_collector.emit(entry.getKey(),input, values);
			}
			catch(Exception  e){
				logger.error("emit values error " + e.getMessage());
			}
		}
		m_isMalloced = false;
		if(m_inputBuff.size() >= m_buffSize){
			m_inputBuff.clear();
		}
	}
	/**
	 * 
	 * 函数名：fillEmitField
	 * 功能描述：从input中取出字段填充字段集
	 * @param input 输入的元组
	 */
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
	/**
	 * 在对象分发到节点后调用的函数
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		m_context = context;
		m_collector = collector;
	}
	/**
	 * 在对象分发到节点后,调用此函数声明流信息
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		for (Entry<String, List<String>> entry: m_outputStream.entrySet()) {
			declarer.declareStream(entry.getKey(), new Fields(entry.getValue()));
		}
	}
}

