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

import com.tracker.api.handler.WebStatsServiceHandler;
import com.tracker.api.thrift.web.AreaFilter;
import com.tracker.api.thrift.web.EntryPageStats;
import com.tracker.api.thrift.web.LogFilter;
import com.tracker.api.thrift.web.PageStats;
import com.tracker.api.thrift.web.ReferrerFilter;
import com.tracker.api.thrift.web.UserFilter;
import com.tracker.api.thrift.web.UserInfo;
import com.tracker.api.thrift.web.UserInfoResult;
import com.tracker.api.thrift.web.UserLog;
import com.tracker.api.thrift.web.UserLogResult;
import com.tracker.api.thrift.web.WebStats;
import com.tracker.api.thrift.web.WebStatsService;
import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.common.constant.website.VisitorType;
import com.tracker.db.constants.DateType;

public class WebStatsServiceTest {
	public static void main(String[] args) throws TException, IOException {
		TSocket socket = new TSocket("10.100.2.93", 44444); 
		socket.setTimeout(100000); // 设置timeout时间
		TTransport transport = new TFramedTransport(socket);
		transport.open();
		TProtocol protocol = new TCompactProtocol(transport);
		
		WebStatsService.Client client = new WebStatsService.Client(new TMultiplexedProtocol(protocol, "WebStatsService"));
		
		
//		testGetWebSiteStats();
//		testGetWebStatsByDate();
//		testGetWebStatsByHour();
//		testGetWebStatsByReferrer();
//		testGetWebStatsByKeyword(client);
//		testGetWebStatsByArea();
//		testGetWebStatsByPage();
//		testGetWebStatsByEntryPage();
//		testGetWebStatsBySysEnv();
		
		testGetUserInfo();
//		testGetUserLog(client);
//		transport.close();
		Servers.shutdown();
	}
	
	public static void testGetUserInfo() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		
		UserFilter userFilter = new UserFilter();
		userFilter.setUserType(3);
//		userFilter.setIp("10.100.50.192");
//		userFilter.setCookieId("1407116845281959903");
		
		
		UserInfoResult result = client.getUserInfos(1, DateType.DAY.getValue(), "20141021", userFilter, 0, 10);
		System.out.println("totalCount:" + result.getTotalCount());
		for(UserInfo log: result.getUserInfoList()){
			System.out.println(log);
		}
	}
	
	public static void testGetUserLog(WebStatsService.Client client) throws TException{
//		WebStatsServiceHandler client2 = new WebStatsServiceHandler();
		
		UserFilter userFilter = new UserFilter();
//		userFilter.setIp("10.100.50.192");
		userFilter.setUserType(1);
//		userFilter.setCookieId("1403772525430825721");

		LogFilter logFilter = new LogFilter();
		
		UserLogResult result = client.getUserLog(1, DateType.DAY.getValue(), "20141011", userFilter, logFilter, 200, 100);
		System.out.println("totalCount:" + result.getTotalCount() + " => " + result.getUserLogListSize());
//		for(UserLog log: result.getUserLogList()){
//			System.out.println(log);
//		}
	}
	
	public static void testGetWebSiteStats() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		Map<String, WebStats> result = client.getWebSiteStats(1,  DateType.DAY.getValue(), "20141017");
		System.out.println(result);
	}
	
	public static void testGetWebStatsByDate() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		List<String> times = new ArrayList<String>();
		times.add("20141017");
		times.add("20141017");
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		List<WebStats> result = client.getWebStatsForDate(1, DateType.DAY.getValue(), times, userFilter);
		System.out.println(result.size());
		for(WebStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsByHour() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.NEW_VISITOR.getValue());
		List<WebStats> result = client.getWebStatsForHour(1, DateType.DAY.getValue(), "20141017", userFilter);
		System.out.println(result.size());
		for(WebStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsByReferrer() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		
		ReferrerFilter referrerFilter = new ReferrerFilter();
//		referrerFilter.setRefType(ReferrerType.OTHER_LINK.getValue());
//		referrerFilter.setSeDomainId(15);
		 
		List<WebStats> result = client.getWebStatsForReferrer(1, DateType.DAY.getValue(), "20141017",  referrerFilter, null, 0, 0);
		System.out.println(result.size());
		for(WebStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsByKeyword(WebStatsService.Client client) throws TException{
//		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		List<WebStats> result = client.getWebStatsForKeyword(1, DateType.DAY.getValue(), "20140615",  0, userFilter, 0, 0);
		System.out.println(result.size());
		for(WebStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsByArea() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		
		AreaFilter areaFilter = new AreaFilter();
		areaFilter.setCountryId(1);
		areaFilter.setProvinceId(12);
		List<WebStats> result = client.getWebStatsForArea(1, DateType.DAY.getValue(), "20141017", userFilter, areaFilter);
		System.out.println(result.size());
		for(WebStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsByPage() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		
		List<PageStats> result = client.getWebStatsForPage(1, DateType.DAY.getValue(), "20141017", userFilter);
		System.out.println(result.size());
		for(PageStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsByEntryPage() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		
		List<EntryPageStats> result = client.getWebStatsForEntryPage(1, DateType.DAY.getValue(), "20141017", userFilter);
		System.out.println(result.size());
		for(EntryPageStats stats: result){
			System.out.println(stats);
		}
	}
	
	public static void testGetWebStatsBySysEnv() throws TException{
		WebStatsServiceHandler client = new WebStatsServiceHandler();
		UserFilter userFilter = new UserFilter();
//		userFilter.setVisitorType(VisitorType.OLD_VISITOR.getValue());
		
		List<WebStats> result = client.getWebStatsForSysEnv(1, DateType.DAY.getValue(), "20141017", SysEnvType.COLOR_DEPTH.getValue(), userFilter);
		System.out.println(result.size());
		for(WebStats stats: result){
			System.out.println(stats);
		}
	}
}
