package com.tracker.common.log.parser;

public class DefaultLogParser implements LogParser{

	/**
	 * 不转化logStr格式
	 */
	@Override
	public LogResult parseLog(String logStr) {
		return new LogResult(logStr, null);
	}

}
