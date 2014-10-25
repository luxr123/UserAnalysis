package com.tracker.common.data;

import com.tracker.common.data.useragent.Browser;
import com.tracker.common.data.useragent.OperatingSystem;
import com.tracker.common.data.useragent.UserAgent;
import com.tracker.common.data.useragent.UserAgentUtil;

public class UserAgentTest {
	public static void main(String[] args) {
		String str = "Mozilla/5.0 (Windows NT 6.1; rv:29.0) Gecko/20100101 Firefox/29.0";
		str = "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36";
//		UserAgent userAgent = UserAgent.parseUserAgentString(str);
//		System.out.println(userAgent.getBrowser().getName() + " , " + userAgent.getBrowserVersion());
//		System.out.println(userAgent.getOperatingSystem().getName() + " , " + userAgent.getOperatingSystem().getDeviceType().getName());
//		
//		Browser browser = Browser.parseUserAgentString(str);
//		System.out.println(userAgent.getBrowser().getName() + " , " + userAgent.getBrowserVersion());
//		
//		
//		OperatingSystem operatingSystem = OperatingSystem.parseUserAgentString(str);
//		System.out.println(userAgent.getOperatingSystem().getName() + " , " + userAgent.getOperatingSystem().getDeviceType().getName());
		
		
		System.out.println(UserAgentUtil.getBrowserByUserAgent(str));
		System.out.println(UserAgentUtil.getOSByUserAgent(str));
	}
}
