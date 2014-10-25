package com.tracker.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {
	private static Logger logger = LoggerFactory.getLogger(DateUtil.class);
	private static ThreadLocal<SimpleDateFormat> apacheDateFormat = new ThreadLocal<SimpleDateFormat>(){
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
		}
	};
	
	private static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>(){
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
	};
	
	/**
	 * 解析时间
	 * Returns the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 * @param original
	 * @return 
	 */
	public static long parseTimeToLong(String original){
		 try {
			return apacheDateFormat.get().parse(original).getTime();
		 } catch (ParseException e) {
		    logger.error("error to parse time: " + original, e); 
		 }
		
		 try {
			 String dateStr = parserTime2(original);
			 return dateFormat.get().parse(dateStr).getTime();
		 } catch (ParseException e) {
			logger.error("error to parse time: " + original, e); 
		 }
		 return System.currentTimeMillis();
	}
	
	/**
	 * 正则匹配解析时间
	 * Z 对于格式化来说，使用 RFC 822 4-digit 时区格式
	 * 参考 http://www.javaweb.cc/JavaAPI1.6/java/text/SimpleDateFormat.html
	 * @param original
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public static String parseTime(String original){
	    try {
	      String date = dateFormat.get().format(apacheDateFormat.get().parse(original));
	      return date;
	    }
	    catch (ParseException e) {
	    	logger.error("error to parse time: " + original, e); 
	    }
	    return parserTime2(original);
	}
	
	private static final HashMap<String, String> monthMap = new HashMap<String, String>();
	{
		monthMap.put("Jan", "01");
		monthMap.put("Feb", "02");
		monthMap.put("Apr", "03");
		monthMap.put("Mar", "04");
		monthMap.put("May", "05");
		monthMap.put("June", "06");
		monthMap.put("July", "07");
		monthMap.put("Aug", "08");
		monthMap.put("Sept", "09");
		monthMap.put("Oct", "10");
		monthMap.put("Nov", "11");
		monthMap.put("Dec", "12");
	}
	
	/**
	 * 字符匹配解析时间
	 */
	private  static String parserTime2(String original){
	    StringBuffer sb = new StringBuffer();
	    original = original.substring(0, original.indexOf(32));
	    String[] parts = original.split("/");
	    int index = parts[2].indexOf(58);
	    String year = parts[2].substring(0, index);
	    String hms = parts[2].substring(index + 1);
	    sb.append(year);
	    sb.append('-');
	    sb.append(monthMap.get(parts[1]));
	    sb.append('-');
	    sb.append(parts[0]);
	    sb.append(" ");
	    sb.append(hms);
	    return sb.toString();
	}
	
	public static String parseTimeToString(long time){
		return dateFormat.get().format(new Date(time));
	}
	
	public static void main(String[] args) {
//		System.out.println(DateUtil.parseTimeToString(System.currentTimeMillis()));
		
		ExecutorService service = Executors.newFixedThreadPool(20);
		for(int i = 0; i < 20; i++){
			service.submit(new Runnable() {
				@Override
				public void run() {
					for(int j = 0; j < 10; j++)
					System.out.println(parseTimeToLong("2014-08-27 00:00:00"));
				}
			});
		}
	}
}
