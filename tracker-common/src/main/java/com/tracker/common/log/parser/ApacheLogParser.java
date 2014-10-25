package com.tracker.common.log.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.log.ApacheLog;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.utils.DateUtil;
import com.tracker.common.utils.JsonUtil;

/**
 * 解析apache日志
 * @author jason.hua
 *
 */
public class ApacheLogParser implements LogParser{
	private static final Logger LOG = LoggerFactory.getLogger(ApacheLogParser.class);
	//正则匹配apachelog
	private final static String APACHE_LOG_REGEX = "([0-9\\.]+) \\[(.*)\\] \"(.+).gif\\?(.*) HTTP.*\" (.+) \"(.*)\"";
//	private final static String APACHE_LOG_REGEX = "([0-9\\.]+) \\[(.*)\\] \"(.+).gif\\?(.*)[^\"]*\"( [.+] )*([\".*\"])*";
	private final static int APACHE_LOG_FIELDS_COUNT = 6;//apache log field number
	private final static int IP_INDEX= 0; //ip
	private final static int LOG_TIME_INDEX = 1; //logTime
	private final static int IMAGE_INDEX = 2; //image name
	private final static int REQUEST_INDEX = 3; //request
	private final static int VISIT_STATUS_INDEX = 4; //visitStatus
	private final static int USER_AGENT_INDEX = 5; //userAgent
	
	/**
	 * 解析日志，并转为json格式
	 */
	@Override
	public LogResult parseLog(String logStr){
		if(logStr == null || logStr.length() == 0)
			return null;
		String[] strs = parseLog(APACHE_LOG_REGEX, logStr, APACHE_LOG_FIELDS_COUNT);
		if(strs == null)
			return null;
		ApacheLog log = ApacheLog.getApacheLog(logStr);
		log.setData(strs[IP_INDEX], DateUtil.parseTimeToLong(strs[LOG_TIME_INDEX]), strs[USER_AGENT_INDEX],strs[VISIT_STATUS_INDEX], strs[REQUEST_INDEX]);
		String json = JsonUtil.toJson(log);
		
		boolean isCorrectLog = log.cleanLog();
		if(isCorrectLog){
			return new LogResult(json, log.getLogType());
		} else {
			LOG.warn("lack field, log => " + json);
		}
		return null;
	}

	/**
	 * 解析日志
	 * @param regex 正则表达式
	 * @param str 需要匹配的字符串
	 * @param count 获取参数的数量
	 * @return
	 */
	private String[] parseLog(String regex, String str, int count){
		Matcher m = Pattern.compile(regex).matcher(str);
		String[] params = null;
		if (m.find()) {
			params = new String[count];
			for(int i = 0; i < params.length; i++){
				params[i] = m.group(i + 1);
			}
		}
		return params;
		
	}
	
	/**
	 * test 
	 */
	public static void main(String[] args) {
		ApacheLogParser parser = new ApacheLogParser();
		String line = "10.100.50.73 [20/Jun/2014:14:21:34 +0800] \"GET /tjpv.gif?webId=1&uid=286&utype=2&ckid=1403245225348607029&ckct=1403245225&cd=32&ck=1&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fspyinfo.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fspyauth.php&tl=%E7%8C%8E%E5%A4%B4%E8%AE%A4%E8%AF%81-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1403245324942&reftype=1&refd=&refsubd=&refkw= HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36\"";
		
		LogResult result = parser.parseLog(line);
		System.out.println(result.getLogJson());
		System.out.println(result.getLogTypeMappring());
		ApachePVLog pvlog = JsonUtil.toObject(result.getLogJson(), ApachePVLog.class);
		System.out.println(pvlog.getIp());
	}
}
