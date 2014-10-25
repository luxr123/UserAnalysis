package com.tracker.common.data.useragent;

/**
 * utils for parse userAgent
 * @author jason.hua
 *
 */
public class UserAgentUtil {
	/**
	 * 获取操作系统名
	 * @param userAgent
	 * @return
	 */
	public static String getOSByUserAgent(String userAgent){
		OperatingSystem operatingSystem = OperatingSystem.parseUserAgentString(userAgent);
		return operatingSystem.getName();
	}
	
	/**
	 * 获取浏览器名
	 * @param userAgent
	 * @return
	 */
	public static String getBrowserByUserAgent(String userAgent){
		Browser browser = Browser.parseUserAgentForParent(userAgent);
		if(browser == Browser.IE){
			browser = Browser.parseUserAgentForChild(userAgent);
		}
		return browser.getName();
	}
	
	public static void main(String[] args) {
		System.out.println(UserAgentUtil.getBrowserByUserAgent("Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0"));
		System.out.println(UserAgentUtil.getBrowserByUserAgent("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2; Trident/4.0; .NET CLR 1.1.4322; InfoPath.1; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET4.0C; .NET4.0E)"));
	}
}
