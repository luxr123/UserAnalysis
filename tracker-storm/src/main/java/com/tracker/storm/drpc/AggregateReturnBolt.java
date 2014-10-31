package com.tracker.storm.drpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.storm.drpc.drpcresult.DrpcResult;

import backtype.storm.drpc.ReturnResults;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
/**
 * 
 * 文件名：AggregateReturnBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:56:53
 * 功能描述：聚合Drpc请求的分布式结果,进行合并后返回给drpc服务器
 *
 */
public class AggregateReturnBolt extends ReturnResults{

	protected HashMap<Object,List<Object> > m_resultes;
	protected String m_compentId;
	protected TopologyContext m_context;
	protected OutputCollector m_collector;

	public static final String IPCOLUMN = "ipcounts";
	public static final String UVCOLUMN = "uvcounts";
	public static final String SEARCHCOLUMN = "searchcounts";
	public static final String FIRSTTIME = "firstVisitTime";
	public static final String LASTTIME = "lastVisitTime";
	public static final String COUNT = "count";
	
	public AggregateReturnBolt(String compentId){
		m_compentId = compentId;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		m_resultes = new HashMap<Object,List<Object>>();
		m_context = context;
		m_collector = collector;
		
	}
	/**
	 * 获取请求的唯一id值,1.尚未缓存该id的key-value,放入缓存,结束.
	 * 2.缓存中有该id,取出缓存的结果集数量,对比请求中的结果总数,a.
	 * 等于结果数,合并结果集,返回合并对象到drpc服务器.b.小于结果数
	 * ,放入缓存,结束.
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		List<Object> sets = null;
		//
		 //input.getValue(1) contain the request ID,which is unique
		if(m_resultes.containsKey(input.getValue(1))){
			sets = m_resultes.get(input.getValue(1));
			Integer retCount = input.getIntegerByField("totalcount");
			sets.add(input.getValue(0));
			DrpcResult ret = null;
			//all results are buffered,merge the result
			if(sets.size() == retCount){
				for(Object obj:sets){
					if(obj == null)
						continue;
					DrpcResult tmp = (DrpcResult)obj;
					if(ret == null){
						ret = tmp;
					}else{
						ret.merge(tmp);
					}
				}
				Integer responseType = 0;
				if(ret != null){
					responseType = ret.responseType();
					switch(responseType){
					case 1000: //its a magic number,just for issue
						
						break;//for secondary or more compute
					default:
						if(!input.getString(1).equals(""))	// for test, that does not contain ID,so will not response to DRPC server
							sendResult(input,ret);
					}
				}else{
							sendResult(input,null);
				}
				//remove the cached result
				m_resultes.remove(input.getValue(1));
			}
		}else{	
			DrpcResult result = (DrpcResult)input.getValue(0);
			if(null != result && result.responseType() < 10){// just request ,no need compute,return the result to DrpcServer
				sendResult(input,result);
			}else{//buffer the partial result
				m_resultes.put(input.getValue(1), new ArrayList<Object>());
				m_resultes.get(input.getValue(1)).add(input.getValue(0));
			}
		}
	}
	public void sendResult(Tuple input,DrpcResult result){
		String retStr = "";
		if (result != null)
			retStr = result.toString();
		super.execute(new TupleImpl(m_context, new Values(retStr,
				input.getString(1),0), input.getSourceTask(), input.getSourceStreamId()));
	}
}

