package com.tracker.flume.source.realtime;

import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.common.log.parser.LogParser;

public class Test {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		LogParser logParser = null;
			logParser = (LogParser) Class.forName(ApacheLogParser.class.getName()).newInstance();
		System.out.println(logParser.parseLog(""));
		
//		ZkUtils.maybeDeletePath("10.100.2.92,10.100.2.93,10.100.2.94", "/consumers/" + 1);
	}
}
