package com.tracker.storm.drpc;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tracker.common.log.ApacheSearchLog.FIELDS;
import com.tracker.storm.common.basebolt.BaseBolt;
import com.tracker.storm.drpc.drpcprocess.DrpcProcess;
import com.tracker.storm.drpc.drpcresult.DefaultResult;
import com.tracker.storm.drpc.drpcresult.DrpcResult;
import com.tracker.storm.drpc.spottype.ClassItem;
import com.tracker.storm.drpc.spottype.DynamicConstruct;
/**
 * 
 * 文件名：SearchRealTimeStatistic
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:59:03
 * 功能描述：针对drpc请求,调用对应的类去处理,并将结果发送给AggregateReturnBolt.
 *
 */
public class SearchRealTimeStatistic extends BaseBolt{

	/**
	 * 
	 */
	private HashMap<String,DrpcProcess> m_process;//请求处理列表 
	private HashMap<String,ClassItem> m_initTable;//处理类初始化信息列表,用于动态创建处理类
	private static final long serialVersionUID = -2793151694058271085L;
	public static final String m_slogFields[] = {FIELDS.webId.toString(),FIELDS.cookieId.toString(),FIELDS.ip.toString()};
	public static final String STREAMID = "rtstatisticBolt";
	
	public SearchRealTimeStatistic(){
		m_process = new HashMap<String, DrpcProcess>();
		m_initTable = new HashMap<String, ClassItem>();
	}
	/**
	 * 根据请求的名称,调用对应缓存对象的process()函数,返回DrpcResult对象,传输到下一个bolt
	 * 如果请求名称美欧有对应的处理对象,着构造一个默认的DrpcResult传输到下一个bolt
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String request = input.getString(0);
		String splits[] = input.getString(0).split("-"); //splits[0] is requesting object name
		DrpcResult output = null;
		if(m_process.containsKey(splits[0]) &&
				m_process.get(splits[0]).isProcessable(request)){
			output = m_process.get(splits[0]).process(request,null);
		}else{
			output = new DefaultResult("unknow request: " + request);
			
		}
		m_collector.emit(STREAMID, new Values(output,input.getValue(1),input.getValue(2)));
	}
	/**
	 * 初始化处理类对象,添加该对象到请求处理列表
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		for(Entry<String,ClassItem>  element:m_initTable.entrySet()){
			try {
				//利用java的反射机制创建新的对象
				DrpcProcess obj = (DrpcProcess)element.getValue().getObject().newInstance();
				//调用init函数初始化对象的值
				if(DynamicConstruct.class.isInstance(obj)){
					DynamicConstruct dc = (DynamicConstruct)obj;
					dc.init(element.getValue().getArgs());
				}
				m_process.put(element.getKey(), obj);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/**
	 * 
	 * 函数名：addProcessItem
	 * 功能描述：缓存请求处理的信息,当框架调用prepare()函数后进行实例化
	 * @param request 请求的名称
	 * @param item 处理请求的类信息
	 * @param args 处理请求的类初始化参数
	 */
	public<T extends DrpcProcess> void addProcessItem(String request,Class<T> item,Object[] args){
		m_initTable.put(request, new ClassItem<T>(args, item));
	}
	/**
	 * 
	 * 函数名：addProcesser
	 * 功能描述：向bolt的请求处理列表添加对象
	 * @param request
	 * @param object
	 */
	private void addProcesser(String request,DrpcProcess object){
		m_process.put(request, object);
	}
	
	public static String getCompentId(){
		return "searchRealtimeBolt";
	}

	public static String getStreamId(){
		return STREAMID;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream(STREAMID, new Fields("value","ids","totalcount"));
	}
}
