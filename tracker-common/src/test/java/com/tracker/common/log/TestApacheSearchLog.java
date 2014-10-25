package com.tracker.common.log;

import java.util.Map;

import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.common.log.parser.LogParser;
import com.tracker.common.log.parser.LogParser.LogResult;
import com.tracker.common.utils.JsonUtil;
import com.tracker.common.utils.StringUtil;

public class TestApacheSearchLog {
	public static void main(String[] args) {
		String log = "10.100.50.163 [16/Jul/2014:13:28:14 +0800] \"GET /tjsearch.gif?webId=1&uid=&utype=&ckid=1405070387433365460&ckct=1405070387&ip=10.100.50.163&url=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&cat=FoxEngine&vt=1405488494762&type=1&area=&companySize=&companyText=&companyType=&corePos=&company=&degree=&fullText=&hisCompany=&industry=&posLevel=&posText=1112&sex=&workYear=&div=&niscohis=0&nisseniordb=0&responseTime=undefined&totalCount=undefined&resultCount=undefined&curPage=&searchType=1 HTTP/1.1\" 200 \"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0\"";
		
//		log = "10.100.10.113 [18/Sep/2014:15:04:02 +0800] \"GET /tjsearch.gif?webId=1&uid=3217&utype=2&ckid=1410410219860079134&ckct=1410410219&ip=10.100.50.192&cd=32&ck=true&la=zh-CN&sc=1680x1050&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchcase.php%3Fcasetype%3Dfoxhrcase&tl=%E6%89%BECase-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1411023947779&cat=CaseEngine&type=2&isCallSE=true&alltext=0&prepaid=0&exclusive=0&responseTime=1&totalCount=61&resultCount=10&curPage=1&searchParam=0%100%100%100%100%100%10%100%100%101%101%10100%10%10 HTTP/1.0\" 200 \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36 CoolNovo/2.0.9.20\"";
		
//		log = "10.100.10.111 [24/Sep/2014:17:16:30 +0800] \"GET /tjsearch.gif?webId=1&uid=3217&utype=2&ckid=1411537558483113156&ckct=1411537558&ip=10.100.50.192&cd=32&ck=true&la=zh-CN&sc=1680x1050&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php%3Fposition%3D%25CB%25AE%25B5%25E7%25B7%25D1%25CA%25A6%25B8%25B5%26company%3D%26areatitle%3D%26areaname%3D%25BE%25D3%25D7%25A1%25B5%25D8%26area%3D%26fulltext%3D&tl=%E6%89%BE%E7%B2%BE%E8%8B%B1-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1411550301564&cat=FoxEngine&type=1&posText=%E6%B0%B4%E7%94%B5%E8%B4%B9%E5%B8%88%E5%82%85&niscohis=0&nisseniordb=0&responseTime=81&searchType=1&searchParam=1%101%100%100%109%100%100%100%100%100%100%100%100%100%100%100%101%10100%10%E6%B0%B4%E7%94%B5%E8%B4%B9%E5%B8%88%E5%82%85%10%10&isCallSE=true HTTP/1.0\" 200 \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36 CoolNovo/2.0.9.20\"";
		
		log = "10.100.10.111 [24/Sep/2014:15:23:46 +0800] \"GET /tjsearch.gif?webId=1&uid=300&utype=2&ckid=1401085695158483360&ckct=&ip=10.100.50.15&cd=24&ck=true&la=zh-CN&sc=1280x768&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchcase.php%3Fcasetype%3Dfoxspycase&tl=%E6%89%BECase-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1411543537766&cat=CaseEngine&type=1&isCallSE=true&alltext=0&responseTime=6&totalCount=216&resultCount=10&curPage=1&searchParam=0%100%100%100%100%100%10%100%100%100%101%10100%10%10 HTTP/1.0\" 200 \"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36\"";
		
//		log = "10.100.10.111 [08/Oct/2014:18:10:08 +0800] \"GET /tjsearch.gif?webId=1&uid=&utype=3&ckid=1411971635253874686&ckct=1411971635&ip=10.100.50.82&cd=24&ck=true&la=zh-cn&sc=1440x900&u=http%3A%2F%2Fjy.51job.com%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&tl=%E6%89%BE%E7%B2%BE%E8%8B%B1-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1412763082245&cat=FoxEngine&type=1&area=120200&companyText=alert(%22done%22)%3B&fullText=alert(%22done%22)%3B&niscohis=0&nisseniordb=0&responseTime=3&curPage=1&searchType=1&searchParam=1%101%100%100%109%101%10120200%100%100%100%100%100%100%100%100%100%100%101%10100%10%10alert(%26quot%3Bdone%26quot%3B)%3B%10alert(%26quot%3Bdone%26quot%3B)%3B&isCallSE=true HTTP/1.0\" 200 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; InfoPath.1; .NET4.0C; .NET4.0E)\"";
		
//		log = "10.100.10.111 [10/Oct/2014:15:28:17 +0800] \"GET /tjsearch.gif?webId=1&uid=&utype=3&ckid=1412903623491980526&ckct=1412903623&ip=10.100.2.145&cd=16&ck=true&la=zh-cn&sc=1229x768&u=http%3A%2F%2Fjy.51job.com%2Fspy%2Fsearchmanager.php&tl=%E6%89%BE%E7%B2%BE%E8%8B%B1-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1412926165202&cat=FoxEngine&type=1&posText=alert(%22done%22)%3B&niscohis=0&nisseniordb=1&responseTime=1&curPage=1&searchType=1&searchParam=1%101%101%100%109%100%100%100%100%100%100%100%100%100%100%100%101%10100%10alert(%26quot%3Bdone%26quot%3B)%3B%10%10&isCallSE=true HTTP/1.0\" 200 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)\"";
		
		log = "10.100.10.113 [17/Oct/2014:14:14:46 +0800] \"GET /tjsearch.gif?webId=1&uid=451&utype=2&ckid=1413516898155834561&ckct=1413516898&ip=10.100.50.34&cd=32&ck=true&la=zh-cn&sc=1440x900&u=http%3A%2F%2Fjy.51job.com%2Fspy%2Fsearchcase.php%3Fcasetype%3Dfoxhrcase&tl=%E6%89%BECase-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1413526535467&cat=CaseEngine&type=2&isCallSE=false&caseName=716&alltext=0&prepaid=0&exclusive=0&totalCount=1&resultCount=1&curPage=1 HTTP/1.0\" 200 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; InfoPath.1; .NET4.0C; .NET4.0E)\"";
		
		LogParser logParser = new ApacheLogParser();
		LogResult result = logParser.parseLog(log);
		System.out.println(result.getLogTypeMappring());
		System.out.println(result.getLogJson());
		System.out.println(JsonUtil.parseJSON2Map(result.getLogJson()));
		
		Map<String, Object> map = JsonUtil.parseJSON2Map(result.getLogJson());
		for(String key: map.keySet()){
			System.out.println(key + " => " + map.get(key));
		}
		
		ApacheSearchLog searchLog = JsonUtil.toObject(result.getLogJson(), ApacheSearchLog.class);
//		String searchConditionJson = StringUtil.tarnsferStr(searchLog.getSearchConditionJson());
		System.out.println(JsonUtil.parseJSON2Map(searchLog.getSearchConditionJson()));
//		
//		if(searchLog.getCategory().equalsIgnoreCase("FoxEngine")){
//			ManagerSearchCondition condition = JsonUtil.toObject(searchLog.getSearchConditionJson(), ManagerSearchCondition.class);
//			System.out.println(JsonUtil.toJson(condition));
//		} else 	if(searchLog.getCategory().equalsIgnoreCase("CaseEngine")){
//			CaseSearchCondition condition = JsonUtil.toObject(searchLog.getSearchConditionJson(), CaseSearchCondition.class);
//			System.out.println(JsonUtil.toJson(condition));
//		}
	}
}
