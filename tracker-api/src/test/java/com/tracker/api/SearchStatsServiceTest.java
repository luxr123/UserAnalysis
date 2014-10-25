package com.tracker.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.tracker.api.handler.SearchStatsServiceHandler;
import com.tracker.api.thrift.search.SearchEngineParam;
import com.tracker.api.thrift.search.SearchStats;
import com.tracker.api.thrift.search.SearchStatsResult;
import com.tracker.api.thrift.search.SearchStatsService;
import com.tracker.api.thrift.search.TopResponseTimeResult;
import com.tracker.api.thrift.search.TopResponseTimeStats;
import com.tracker.common.constant.search.SearchResultType;
import com.tracker.db.constants.DateType;

public class SearchStatsServiceTest {
	public static void main(String[] args) throws TException, IOException {
		TSocket socket = new TSocket("10.100.2.93", 44444); 
		socket.setTimeout(100000); // 设置timeout时间
		TTransport transport = new TFramedTransport(socket);
		transport.open();
		TProtocol protocol = new TCompactProtocol(transport);
		
//		WebStatsService.Client client = new WebStatsService.Client(protocol);
		SearchStatsService.Client client = new SearchStatsService.Client(new TMultiplexedProtocol(protocol, "SearchStatsService"));
		
//		testGetSearchStats();
//		testGetTotalSearchCount();
//		testGetSearchResultStats();
//		testGetSearchConditionStats();
		testGetSearchPageStats();
//		testGetSearchStatsForDate();
//		
//		testGetSearchValueStats();
//		testGetTopResponseTimeResult();
//		testGetTopIpResult(client);
		
		transport.close();
//		Servers.shutdown();
	}
	
	public static void testGetTotalSearchCount() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);

		long result = client.getTotalSearchCount(1, 1, "20141017", param);
		System.out.println(result);
	}
	
	public static void testGetSearchResultStats() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);

		List<SearchStats> result = client.getSiteSearchStats(1, 1, "20141017", param, SearchResultType.SEARCH_TIME.getType());
		System.out.println(result.size());
		for(SearchStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetSearchConditionStats() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);

		List<SearchStats> result = client.getSiteSearchStats(1, 1, "20141017", param, 0);
		System.out.println(result.size());
		for(SearchStats stats: result){
			System.out.println(stats);
		}
	}
	
	
	public static void testGetSearchValueStats() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);
		
		SearchStatsResult result = client.getSearchValueStats(1, 1, "20141011", param, 16, 0, 10);
		
		System.out.println(result.totalCount);
		for(SearchStats stats: result.statsList){
			System.out.println(stats);
		}
	}
	
	public static void testGetSearchPageStats() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);

		List<SearchStats> result = client.getSearchPageStats(1, 1, "20141017", param);
		System.out.println(result.size());
		for(SearchStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetTopResponseTimeResult() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);
		
		TopResponseTimeResult result = client.getTopResponseTimeResult(1, 1, "20140923", param,  50, 100);
		
		System.out.println(result.totalCount);
		for(TopResponseTimeStats stats: result.statsList){
			System.out.println(stats);
		}
	}
	
	public static void testGetSearchStatsForDate() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);
		
		List<String> times = new ArrayList<String>();
		times.add("20140808");
		times.add("20140812");
		List<SearchStats> result = client.getSearchStatsForDate(1, 1, times, param);
		
		System.out.println(result.size());
		for(SearchStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetTopIpResult(SearchStatsService.Client client) throws TException{
//		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		SearchEngineParam param = new SearchEngineParam();
		param.setSearchEngineId(1);
		param.setSearchType(1);
		
		SearchStatsResult result = client.getTopIpResult(1, 1, "20141015", param, 0, 1);
		System.out.println(result.totalCount);
		for(SearchStats stats: result.statsList){
			System.out.println(stats);
		}
	}
	
	public static void testGetRealTimeSearchStats() throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		Map<String, SearchStats> result = client.getSearchStats(1, 1, "20140812");

		for(String key: result.keySet()){
			System.out.println(key + " => " + result.get(key));
		}
	}
	
	public static void testGetSearchStats()throws TException{
		SearchStatsServiceHandler client = new SearchStatsServiceHandler();
		Map<String, SearchStats> result = client.getSearchStats(1, DateType.DAY.getValue(), "20141017");
		System.out.println(result);
	}
}
