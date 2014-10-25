package com.tracker.hbase.service.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.tracker.common.utils.DateUtils;
import com.tracker.db.constants.DateType;

/**
 * 文件名：TimeUtil
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午4:06:28
 * 功能描述：根据timeType和time获取日期集合
 *
 */
public class TimeUtil {
	
	/**
	 * 根据timeType和time获取日期集合
	 * @param timeType
	 * @param time
	 * @return
	 */
	public static List<String> getTimes(Integer timeType, String time) {
		List<String> times = new ArrayList<String>();
		
        if(timeType == DateType.WEEK.getValue()){
			int year = Integer.parseInt(time.substring(0, 4));
			int week = Integer.parseInt(time.substring(4));
			
		    Calendar calendar = Calendar.getInstance(); 
			calendar.clear(); 
			calendar.set(Calendar.YEAR, year); 
			calendar.set(Calendar.WEEK_OF_YEAR, week); 
			
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY); 
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			//指定周的第一天
			String startDate = sdf.format(calendar.getTime());  
			 
			calendar.set(Calendar.WEEK_OF_YEAR, (week + 1)); 
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			//指定周的最后一天
			String endDate = sdf.format(calendar.getTime()); 
				
			//获取昨天的日期
			String yesterday = sdf.format(new Date(System.currentTimeMillis() - 24*60*60*1000));
			//如果指定周的最后一天在昨天之后，则指定周的最后一天取昨天（适用于本周的情况）
			if (yesterday.compareTo(endDate) < 0) {
				endDate = yesterday;
			}
			
			times = DateUtils.getDays(startDate, endDate);
		} else if(timeType == DateType.MONTH.getValue()){
			int year = Integer.parseInt(time.substring(0, 4));
			int month = Integer.parseInt(time.substring(4));
			
		    Calendar calendar = Calendar.getInstance(); 
			calendar.clear(); 
			calendar.set(Calendar.YEAR, year);
			calendar.set(Calendar.MONTH, (month - 1));
			
			calendar.set(Calendar.DAY_OF_MONTH,calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			//指定月的第一天
			String startDate = sdf.format(calendar.getTime()); 
			
			calendar.set(Calendar.DAY_OF_MONTH,calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
			//指定月的最后一天
			String endDate = sdf.format(calendar.getTime()); 
			
			//获取昨天的日期
			String yesterday = sdf.format(new Date(System.currentTimeMillis() - 24*60*60*1000));
			//如果指定月的最后一天在昨天之后，则指定月的最后一天取昨天（适用于本月的情况）
			if (yesterday.compareTo(endDate) < 0) {
				endDate = yesterday;
			}
			
			times = DateUtils.getDays(startDate, endDate);
		} else if(timeType == DateType.YEAR.getValue()){
			int year = Integer.parseInt(time);
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
			String today = sdf.format(new Date());
			int todayYear = Integer.parseInt(today.substring(0, 4));
			int todayMonth = Integer.parseInt(today.substring(4));
			
			//如果year是本年，endMonth取本月
			int endMonth = 12;
			if(year == todayYear) {
				if (todayMonth < 12) {
					endMonth = todayMonth;
				}
			}
			
			for (int i = 1; i <= endMonth; i++) {
				if (i < 10) {
					times.add(String.valueOf(year) + "0" + String.valueOf(i));
				} else {
					times.add(String.valueOf(year) + String.valueOf(i));
				}
			}
		}
        
		return times;
	}
	 
	
	public static void main(String[] args) {
		// List<String> times = getTimes(2, "201439");
		// List<String> times = getTimes(3, "201409");
		// ist<String> times = getTimes(4, "2014");

		// List<String> times = getTimes(2, "201438");
		 List<String> times = getTimes(3, "201405");
		// List<String> times = getTimes(4, "2013");

		for (String string : times) {
			System.out.println(string);
		}
	}
}
