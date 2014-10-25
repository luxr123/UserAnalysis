package com.tracker.common.log.parser;

/**
 * 日志解析类
 * @author jason.hua
 *
 */
public interface LogParser {
	/**
	 * 转化日志形式
	 * @param logStr
	 * @return
	 */
	public LogResult parseLog(String logStr);
	
	
	public static class LogResult {
		private String logJson;
		private String logTypeMappring = null;
		
		public LogResult(String logJson, String logTypeMappring){
			this.logJson = logJson;
			this.logTypeMappring = logTypeMappring;
		}

		public String getLogJson() {
			return logJson;
		}

		public void setLogJson(String logJson) {
			this.logJson = logJson;
		}

		public String getLogTypeMappring() {
			return logTypeMappring;
		}

		public void setLogTypeMappring(String logTypeMappring) {
			this.logTypeMappring = logTypeMappring;
		}
	}
}
