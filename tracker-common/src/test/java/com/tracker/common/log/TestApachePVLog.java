package com.tracker.common.log;

import java.util.Map;

import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.common.log.parser.LogParser;
import com.tracker.common.log.parser.LogParser.LogResult;
import com.tracker.common.utils.JsonUtil;

public class TestApachePVLog {
	public static void main(String[] args) {
		String log = "10.100.10.112 [10/Oct/2014:13:26:33 +0800] \"GET /tjpv.gif?webId=1&uid=445&utype=2&ckid=1411876217696970996&ckct=1411876217&ip=10.100.50.155&cd=32&ck=true&la=zh-cn&sc=1440x900&re=http%3A%2F%2Fjy.51job.com%2Fmanager%2Fmanagerprofile.php&u=http%3A%2F%2Fjy.51job.com%2Fspy%2Foffline.php&tl=%E5%BC%BA%E5%88%B6%E4%B8%8B%E7%BA%BF-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1412918870390&reftype=1 HTTP/1.0\" 200 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; InfoPath.1; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET4.0C; .NET4.0E)\"";
		log = "10.100.10.113 [10/Oct/2014:13:29:56 +0800] \"GET /tjpv.gif?webId=1&uid=142&utype=2&ckid=1407133372024196298&ckct=&ip=10.100.50.34&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2Fjy.51job.com%2Fspy%2Fwebchat.php&u=http%3A%2F%2Fjy.51job.com%2Fmanager%2Fcv.php%3Fact%3DshowCv%26managerId%3D1500092240%26caseid%3D0%26isenglish%3D0%26passkey%3D0daf7aced626306d53d6a2367760cbd6&tl=testapple2(1500092240)-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1412919064797&reftype=1 HTTP/1.0\" 200 \"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0\"";
		
		log = "10.100.10.114 [14/Oct/2014:09:42:52 +0800] \"GET /tjpv.gif?webId=1&uid=&utype=3&ckid=1410757027559049565&ckct=1410757027&ip=10.100.50.141&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2Fjy.51job.com%2Fmanager%2Fcv.php%3Fact%3DshowCv%26managerId%3D1500001229%26caseid%3D0%26isenglish%3D0%26passkey%3Dcb727a2591ff17026baec98840367379%26posdiv%3D%26company%3D%26fulltext%3D&u=http%3A%2F%2Fjy.51job.com%2F&tl=%E9%A6%96%E9%A1%B5-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1413251029974&reftype=1 HTTP/1.0\" 200 \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36\"";
		LogParser logParser = new ApacheLogParser();
		
		LogResult result = logParser.parseLog(log);
		System.out.println(result.getLogTypeMappring());
		System.out.println(result.getLogJson());
		System.out.println(JsonUtil.parseJSON2Map(result.getLogJson()));
		
		Map<String, Object> map = JsonUtil.parseJSON2Map(result.getLogJson());
		for(String key: map.keySet()){
			System.out.println(key + " => " + map.get(key));
		}
		
		ApachePVLog logObj = JsonUtil.toObject(result.getLogJson(), ApachePVLog.class);
//		System.out.println(logObj);
	}
}
