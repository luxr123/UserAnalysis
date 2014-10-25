package com.tracker.common.utils;
import com.tracker.common.data.useragent.Browser;
import com.tracker.common.data.useragent.OperatingSystem;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 
 * 文件名：StringUtil
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:08:00
 * 功能描述：字符串工具类,定义字符串分隔符,json格式到keyValue的解析,useragent解析等操作
 *
 */
public class StringUtil {
	
	public static final String KEY_VALUE_SPLIT = ":";
	public static final String RETURN_ITEM_SPLIT = "\n";
	public static final String ARUGEMENT_SPLIT = "-";
	public static final String ARUGEMENT_END = "!!";
	public static final String PATH_SPLIT = "/";

	
	/**
	 * 填充后缀字符串，使其达到{@code size}大小
	 * @param msgSize
	 * @return
	 */
	public static String fillRightData(String data, int size, char filledChar) {
		String result = String.format("%1$-" + size + "s", data).replace(' ', filledChar);
		return result;
	}
	
	/**
	 * 填充前缀字符串，使其达到{@code size}大小
	 * @param msgSize
	 * @return
	 */
	public static String fillLeftData(String data, int size, char filledChar) {
		String result = String.format("%1$" + size + "s", data).replace(' ', filledChar);
		return result;
	}
	
	public static String fillLeftData(int num) {
		return String.format("%02d", num);
	}
	
	public static void split(String strs,String sep,List<String> output){
		String key="",value="";
		if(strs != null){
			try{
				key = strs.substring(0,strs.indexOf(":"));
				value = strs.substring(strs.indexOf(":") + 1,strs.length());
			}
			catch(StringIndexOutOfBoundsException e){
				if(!strs.contains(":")){
					key = null;
					value = strs;
				}
			}
		}
		output.add(key);
		output.add(value);
	}
	/**
	 * 
	 * 函数名：parseKV
	 * 功能描述：对传入的的key:value,key1:value1字符串使用boyer-moore算法进行解析,
	 * @param inputStr
	 * @param IFS
	 * @return
	 */
	public static Map<String,Object> parseKV(String inputStr,String IFS){
		//parse the input format : IP:xxx.xxx.xxx.xxx\tCONTEXT:"Https://head Get"\t............
		Map<String, Object> map = new HashMap<String,Object>();
		String key,value;
		String str = inputStr;
		int beginIndex =0,endIndex =0;
		while(true){
			endIndex = str.indexOf(IFS,endIndex);
			if(endIndex == -1){
				endIndex = str.length();
			}
			else if(str.charAt(endIndex + 1) != '"'){
				endIndex = endIndex + 1;
				continue;
			}
			String subStr = str.substring(beginIndex, endIndex);
			try{
				key = subStr.substring(0,subStr.indexOf(":"));
				value = subStr.substring(subStr.indexOf(":") + 1,subStr.length());
				if(key.contains("\"")){ 
					key =  key.substring(1, key.length() - 1);
				}
				if(value.contains("\"")){
					value = value.substring(1,value.length() -1);
				}
				map.put(key, value);
			}
			catch(Exception e){
				System.out.println("parse error for:" + inputStr);
			}
			try{
				str = str.substring(endIndex + 1);
				endIndex = 0;
			}
			catch(Exception e){
				break;
			}
		}
		return map;
	}
	/**
	 * 
	 * 函数名：parseUserAgent
	 * 功能描述：解析输入网页的useragent字段
	 * @param substr
	 * @return
	 */
	public static List<String> parseUserAgent(String substr){
		//userAgent fomat: Mozilla/5.0 (Windows NT 6.1; rv:29.0) Gecko/20100101 Firefox/29.0            
		List<String> retVal = new ArrayList<String>();
		//parse the browser
		for(Browser toplevelBrowser: Browser.getTopLevelObj()){
			if(isInUseragent(toplevelBrowser.getAliaseName(),toplevelBrowser.getExcludeString(), substr.toLowerCase())){
				//get the parent type
				for(Browser browser: toplevelBrowser.getChild()){
					if(isInUseragent(browser.getAliaseName(),browser.getExcludeString(), substr.toLowerCase())){
						retVal.add(browser.getName());
						break;
					}
				}
				break;
			}
		}
		if(retVal.size() == 0){
			retVal.add("others");
		}
		for(OperatingSystem toplevelOS:OperatingSystem.getTopLevelOS()){
			if(isInUseragent(toplevelOS.getAliaseName(),toplevelOS.getExcludeString(), substr.toLowerCase())){
				for(OperatingSystem os :toplevelOS.getChild()){
					if(isInUseragent(os.getAliaseName(),os.getExcludeString(), substr.toLowerCase())){
						retVal.add(os.getName());
						break;
					}
				}
				//if the toplevelOS has no child
				if(retVal.size() == 1)
					retVal.add(toplevelOS.getName());
				break;
			}
		}
		if(retVal.size() == 1){
			retVal.add("others");
		}
		return retVal;
	}
	private static boolean isInUseragent(String[] alias,String[] excldStr,String substr){
		boolean hasExcld = false;
		if(alias == null)
			return false;
		for(String name:alias){
			if(boyer_mooreSearch(name.toLowerCase(), substr) != -1){
				//check contain exclude String
				if(null != excldStr){
					for(String exclude:excldStr){
						if(boyer_mooreSearch(exclude.toLowerCase(), substr) != -1){
							hasExcld = true;
							break;
						}
					}
				}
				if(!hasExcld){
					return true;
				}else
					break;
			}
		}
		return false;
	}

	/**
	 * 
	 * 函数名：getCurrentDay
	 * 功能描述：获取当天是时间的YY-MM-DD格式日期
	 * @return
	 */
	public static String getCurrentDay(){ 
		Calendar cal = Calendar.getInstance();
		String year = Long.toString(cal.get(Calendar.YEAR));
		String month = "";
		if((cal.get(Calendar.MONTH) + 1 )< 10){
			month = 0 + Long.toString(cal.get(Calendar.MONTH) + 1);
		}else{
			month = Long.toString(cal.get(Calendar.MONTH) + 1);
		}
		String day = "";
		if((cal.get(Calendar.DAY_OF_MONTH))< 10){
			day = 0 + Long.toString(cal.get(Calendar.DAY_OF_MONTH));
		}else{
			day = Long.toString(cal.get(Calendar.DAY_OF_MONTH));
		}
		return year + "-" + month + "-" + day;
	}

	/**
	 * 
	 * 函数名：getDayByMillis
	 * 功能描述：根据输入的毫秒值,返回YY-DD-MM格式的日期
	 * @param millis
	 * @return
	 */
	public static String getDayByMillis(Long millis){ 
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(millis);
		String year = Long.toString(cal.get(Calendar.YEAR));
		String month = "";
		if((cal.get(Calendar.MONTH) + 1 )< 10){
			month = 0 + Long.toString(cal.get(Calendar.MONTH) + 1);
		}else{
			month = Long.toString(cal.get(Calendar.MONTH) + 1);
		}
		String day = "";
		if((cal.get(Calendar.DAY_OF_MONTH))< 10){
			day = 0 + Long.toString(cal.get(Calendar.DAY_OF_MONTH));
		}else{
			day = Long.toString(cal.get(Calendar.DAY_OF_MONTH));
		}
		return year + "-" + month + "-" + day;
	}
	
	public static int boyer_mooreSearch(String input,String sentence){
		int retVal = -1;
		char chars[] = sentence.toCharArray();
		char key[] =  input.toCharArray();
		int keylength = key.length - 1,offset = 0;
		int movtag = keylength;
		while(movtag != -1){
			try{
				if(key[movtag] == chars[movtag + offset]){
					//compare rest letter
					movtag--;
				}
				else{
					int pos = input.lastIndexOf(chars[movtag + offset], movtag);
					if(pos == -1){
						offset += key.length;
					}
					else{
						offset += (movtag - pos);
					}
					movtag = keylength;
				}
			}
			catch(IndexOutOfBoundsException e){
				break;
			}
		}
		if(movtag == -1)
			retVal = offset;
		return retVal;
	}
	
	public static String tarnsferStr(String str){
		if(str == null)
			return null;
		return str.replace("\\", "");
	}
	
	/**
	 * 解析搜索页面url,获取关键词
	 * @param url
	 * @return
	 */
	public static String getMainUrl(String url) {
		if(url == null)
			return null;
		int endIndex = url.indexOf(".php");
		if(endIndex < 0){
			endIndex = url.length();
		} else {
			endIndex += 4;
		}
		return url.substring(0, endIndex);
	}
	
	public static String removeDomain(String url){
		int beginIndex = 0;
		int endIndex = url.length();
		if(url.indexOf("http://") > -1){
			beginIndex += "http://".length();
		} else if(url.indexOf("https://") > -1){
			beginIndex += "https://".length();
		}
		for(int i = beginIndex; i < endIndex; i++){
			if(url.charAt(i) == '/'){
				beginIndex = i + 1;
				break;
			}
		}
		return url.substring(beginIndex, endIndex);
	}
	
	public static String getHost(String url) {
		if(url == null)
			return null;
		int beginIndex = 0;
		if(url.indexOf("http://") > -1){
			beginIndex += "http://".length();
		} else if(url.indexOf("https://") > -1){
			beginIndex += "https://".length();
		}
		int endIndex = 0;
		for(int i = beginIndex; i < url.length(); i++){
			if(url.charAt(i) == '/'){
				endIndex = i;
				break;
			}
		}
		return url.substring(beginIndex, endIndex);
	}
	
	public static boolean isIp(String ip){//判断是否是一个IP 
        boolean b = false; 
      //去掉IP字符串前后所有的空格 
        ip = ip.trim();
        if(ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")){ 
            String s[] = ip.split("\\."); 
            if(Integer.parseInt(s[0])<255) 
                if(Integer.parseInt(s[1])<255) 
                    if(Integer.parseInt(s[2])<255) 
                        if(Integer.parseInt(s[3])<255) 
                            b = true; 
        } 
        return b; 
    } 
	
	public static String getPageName(String url) {
		if(url == null)
			return null;
		int endIndex = url.indexOf("?");
		if(endIndex < 0){
			endIndex = url.length();
		}
		int beginIndex = 0;
		for(int i = endIndex - 1; i >= 0; i--){
			if(url.charAt(i) == '/'){
				beginIndex = i + 1;
				break;
			}
		}
		return url.substring(beginIndex, endIndex);
	}

	public static long parseTimeToLong(String original) {
		DateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		try {
			return dateFormat.parse(original).getTime();
		} catch (ParseException e) {
			
		}
		return System.currentTimeMillis();
	}
	
	
	public static void main(String[] args) {
		System.out.println(getPageName("http://10.100.2.67/spy/public/spy/searchmanager.php?search_kind=accurate"));
	}
}

