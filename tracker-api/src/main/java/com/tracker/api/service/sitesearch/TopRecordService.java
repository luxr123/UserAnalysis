package com.tracker.api.service.sitesearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import com.tracker.common.log.ApacheSearchLog.FIELDS;
import com.tracker.common.utils.ConfigExt;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.redis.JedisUtil;

public class TopRecordService {
	
	private static Logger logger = LoggerFactory.getLogger(TopRecordService.class);
	private Properties m_properties;
	private DRPCClient m_drpcClient;
	
	public TopRecordService(Properties properties){
		m_properties =properties;
		m_drpcClient = new DRPCClient((String)m_properties.get("storm.drpc.server"), Integer.valueOf((String)m_properties.get("storm.drpc.port")));
	}

	public TopRecordResult getTopSearchValue(String webId,String Engine,String searchType,String field,int startIndex, int count){
		return requestToDrpc("topsearchvalue",webId,Engine,searchType,startIndex,count,field);
	}
	
	public TopRecordResult getTopSearchValue(String webId,String Engine,List<String> searchTypes,String field,int startIndex, int count){
		List<TopRecordResult> list = new ArrayList<TopRecordService.TopRecordResult>();
		for(String searchType:searchTypes){
			TopRecordResult trr = requestToDrpc("topsearchvalue",webId,Engine,searchType,startIndex,count,field);
			if(trr != null){
				list.add(trr);
			}
		}
		return merge(list);	}
	
	public TopRecordResult getTopSearchValue(String webId,String Engine,String[] searchTypes,String field,int startIndex, int count){
		return getTopSearchValue(webId, Engine, Arrays.asList(searchTypes), field,startIndex, count);
	}
	
	public TopRecordResult getTopSearchCost(String webId,String Engine,String searchType, int startIndex, int count) {
		return requestToDrpc("toprecord",webId,Engine,searchType,startIndex,count,FIELDS.responseTime.toString());
	}
	
	public TopRecordResult getTopSearchIp(String webId,String Engine,int startIndex, int count) {
		return requestToDrpc("toprecord",webId,Engine,null,startIndex,count,FIELDS.ip.toString());
	}
	
	public TopRecordResult requestToDrpc(String request,String webId,String engine,String searchType, int startIndex, int count,
			String... other) {
		List<TopRecordItem> retVal = new ArrayList<TopRecordService.TopRecordItem>(count);
		Calendar cal = Calendar.getInstance();
		Long total = 0L;
		startIndex -= 1;
		count -= 1;
		if(request.equals("toprecord")){
			//read data from redis
			String requestStr = (cal.get(Calendar.MONTH)  + 1) + StringUtil.ARUGEMENT_SPLIT + cal.get(Calendar.DAY_OF_MONTH)
					+ StringUtil.ARUGEMENT_SPLIT + other[0] + StringUtil.ARUGEMENT_SPLIT+webId+engine;
			if(searchType != null && !other[0].equals("ip")){
				requestStr += StringUtil.ARUGEMENT_SPLIT + searchType;
			}
			Set<String> set = JedisUtil.getInstance(m_properties).SORTSET.zrevrange(requestStr, startIndex, startIndex + count);
			total = JedisUtil.getInstance(m_properties).SORTSET.zlength(requestStr);
			for(String element : set){
				Double score = JedisUtil.getInstance(m_properties).SORTSET.zscore(requestStr, element);
				retVal.add(new TopRecordItem(element,score.longValue()));
			}
		}else if(request.equals("topsearchvalue")){
			if(null == searchType)
				searchType = "";
			String requestStr = request +StringUtil.ARUGEMENT_SPLIT + webId+engine+StringUtil.ARUGEMENT_SPLIT
					+searchType+StringUtil.ARUGEMENT_SPLIT+ other[0] + StringUtil.ARUGEMENT_SPLIT
					+ startIndex + StringUtil.ARUGEMENT_SPLIT + count;
			try {
				//topsearchvalue:webid##engine:searchtype:startindex:endindex
				String response= m_drpcClient.execute("statistic", requestStr);
				if(response == null || !response.contains(StringUtil.RETURN_ITEM_SPLIT))
					return null;
				String tmp  = response.substring(0, response.indexOf(StringUtil.RETURN_ITEM_SPLIT));
				total = Long.parseLong(tmp);
				if(total == 0)
					return null;
				String response_sub = response.substring(response.indexOf(StringUtil.RETURN_ITEM_SPLIT) + 1,
						response.length());
				String[] splits = response_sub.split(StringUtil.RETURN_ITEM_SPLIT);
				for(String element:splits){
					retVal.add(TopRecordItem.parseFrom(element));
				}
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DRPCExecutionException e) {
				// TODO Auto-generated catch block
				logger.error("request exception with: " + requestStr);
			}
		}
		TopRecordResult retResult = new TopRecordResult(retVal, total);
		return retResult;
	}
	
	public static class TopRecordItem{
		private String m_name;
		private Long m_count;
		public TopRecordItem(String name , Long count){
			m_name = name;
			m_count = count;
		}
		public String getName() {
			return m_name;
		}
		public Long getCount() {
			return m_count;
		}
		
		public static TopRecordItem parseFrom(String input){
			String[] values = input.split(StringUtil.KEY_VALUE_SPLIT);
			return new TopRecordItem(values[0], Long.valueOf(values[1]));
		}
	}
	
	public static class TopRecordResult{
		private List<TopRecordItem> m_records;
		private Long m_count;
		
		public TopRecordResult(List<TopRecordItem> list,Long count){
			m_records = list;
			m_count = count;
		}
		
		public Long getCount(){
			return m_count;
		}
		
		public List<TopRecordItem> getList(){
			return m_records;
		}
		
		public void merge(TopRecordResult input){
			m_count += input.getCount();
			Integer size = m_records.size();
			Integer input_size = input.getList().size();
			List<TopRecordItem> tmp = new ArrayList<TopRecordItem>();
			Long count = 0L,input_count = 0L;
			for(int i = 0,j = 0;i<size || j< input_size;){
				count = i >= size ? 0L : m_records.get(i).getCount();
				input_count = j >= input_size ? 0L:input.getList().get(j).getCount();
				if(i > size || (j < input_size && count < input_count)){
					tmp.add(input.getList().get(j++));
				}else if(i < size){
					tmp.add(m_records.get(i++));
				}
				if(tmp.size() > size &&  tmp.size() > input_size){
					break;
				}
			}
			m_records = tmp;
		}
	}
	public static String getResponseCount(String item){
		return parseResponeTimeItem(item).get(2);
	}
	
	public static String getRequestTime(String item){
		return parseResponeTimeItem(item).get(1);
	}
	
	public static String getRequestParam(String item){
		return parseResponeTimeItem(item).get(0);
	}
	
	private static List<String> parseResponeTimeItem(String reItem){
		List<String> retVal = new ArrayList<String>();
		for(String element: reItem.split(StringUtil.KEY_VALUE_SPLIT)){
			retVal.add(element);
		}
		return retVal;
	}
	
	private static TopRecordResult merge(List<TopRecordResult> list){
		TopRecordResult trr = list.get(0);
		for(int i = 1;i < list.size();i++){
			trr.merge(list.get(i));
		}
		return trr;
	}
	
	public static void main(String [] args){
		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
		String configFile = java.lang.System.getenv("COMMON_CONFIG");
		Properties properties = ConfigExt.getProperties(hdfsLocation,configFile);
		TopRecordService trs = new TopRecordService(properties);
		TopRecordResult result = null;
		System.out.println("Top Response Time:");
		result = trs.getTopSearchCost("1", "FoxEngine", "1", 1, 100);
		System.out.println("total count:" + result.getCount());
		for(TopRecordItem element:result.getList()){
			System.out.print( element.getCount() + "\t" + getRequestTime(element.getName())
					+ "\t" + getResponseCount(element.getName())
					+ "\t" + getRequestParam(element.getName()));
			System.out.println();
		}
		result = trs.getTopSearchCost("1", "FoxEngine",null, 1, 10);
		System.out.println("total count:" + result.getCount());
		for(TopRecordItem element:result.getList()){
			System.out.print( element.getCount() + "\t" + getRequestTime(element.getName())
					+ "\t" + getResponseCount(element.getName())
					+ "\t" + getRequestParam(element.getName()));
			System.out.println();
		}
		System.out.println("Top Search Ip:");
		result = trs.getTopSearchIp("1", "FoxEngine", 1, 10);
		System.out.println("total count:" + result.getCount());
		for(TopRecordItem element:result.getList()){
			System.out.println(element.getName() + ":" + element.getCount());
		}
		System.out.println("Top Search posText Value:");
		String[] strArray = {"1","2","3"};
		String searchType = null;
//		result  = trs.getTopSearchValue("1", "FoxEngine", strArray,FIELDS.nisseniordb.toString(),1, 10);
//		result  = trs.getTopSearchValue("1", "FoxEngine", searchType,FIELDS.nisseniordb.toString(),1, 10);
//		if(result != null){
//		System.out.println("total count:" + result.getCount());
//		for(TopRecordItem element:result.getList()){
//			System.out.println(element.getName() + ":" + element.getCount());
//		}
//		}
	}
}
