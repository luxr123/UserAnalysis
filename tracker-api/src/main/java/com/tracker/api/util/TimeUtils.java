package com.tracker.api.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import com.google.common.collect.Lists;
import com.tracker.common.utils.DateUtils;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.constants.DateType;

/**
 * 时间显示方式转换工具类
 * @author jason.hua
 *
 */
public class TimeUtils {
	public static final long MINUTE_INTERVAL = 60 * 1000;
	public static final long HOUR_INTERVAL = 60 * 60 * 1000;
	
	private static ThreadLocal<SimpleDateFormat> minuteSdf = new ThreadLocal<SimpleDateFormat>(){
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy/MM/dd HH:mm");
		}
	};
	
	private static ThreadLocal<SimpleDateFormat> dateSdf = new ThreadLocal<SimpleDateFormat>(){
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy/MM/dd");
		}
	};
	
	private static ThreadLocal<SimpleDateFormat> secondSdf = new ThreadLocal<SimpleDateFormat>(){
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		}
	};
	
	/**
	 * 转为日期形式，精确到天
	 * @param time
	 * @return
	 */
	public static String parseTimeToDate(long time){
		return dateSdf.get().format(new Date(time));
	}
	
	/**
	 * 转为日期形式，精确到分钟
	 * @param time
	 * @return
	 */
	public static String parseTimeToMinute(long time){
		return minuteSdf.get().format(new Date(time));
	}
	
	/**
	 * 转为日期形式，精确到秒
	 * @param time
	 * @return
	 */
	public static String parseTimeToSecond(long time){
		return secondSdf.get().format(new Date(time));
	}
	
	
	/**
	 * YYYYMMDD
	 * @param time
	 * @return
	 */
	public static String getTodayDate(){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(System.currentTimeMillis());
		int month = cal.get(Calendar.MONTH) + 1;
		int day =  cal.get(Calendar.DAY_OF_MONTH);
		int year = cal.get(Calendar.YEAR);
		
		String date = year + StringUtil.fillLeftData(month+"", 2, '0') + StringUtil.fillLeftData(day+"" , 2 ,'0');
		return date;
	}
	
	/**
	 * 页面上展示的时间格式
	 * @param timeType
	 * @param time
	 * @return
	 */
	public static String applyDescForTime(int timeType, String time){
		if(timeType == DateType.DAY.getValue()){
			if(time.length() != 8)
				return time;
			return  DateUtils.applySplitForDate(time.substring(0, 4),time.substring(4,6),time.substring(6,8));
		} else if(timeType == DateType.WEEK.getValue()){
			if(time.length() != 6)
				return time;
			int year = Integer.parseInt(time.substring(0, 4));
			int week = Integer.parseInt(time.substring(4,6));
			String[] dates = DateUtils.getStartAndEndDayByWeek(year, week);
			return year + "/" + time.substring(4,6) + "(" + dates[0] + "-" + dates[1] + ")";
		} else if(timeType == DateType.MONTH.getValue()){
			if(time.length() != 6)
				return time;
			return time.substring(0, 4) + "/" + time.substring(4,6);
		}
		
		return time;
	}
	
	/**
	 * 从起止时间中获取时间集合，并转换为数据库中存储的时间格式
	 * @param timeType
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static List<String> parseTimes(int timeType, String startTime, String endTime){
		if(startTime.compareTo(endTime) > 0){
			String tmpTime = startTime;
			startTime = endTime;
			endTime = tmpTime;
		}
		List<String> tmp = null;
		if(timeType == DateType.DAY.getValue()){
			tmp = DateUtils.getDays(startTime, endTime);
		} else if(timeType == DateType.WEEK.getValue()){
			tmp = DateUtils.getWeeks(startTime, endTime);
		} else if(timeType == DateType.MONTH.getValue()){
			tmp = DateUtils.getMonths(startTime, endTime);
		} else if(timeType == DateType.YEAR.getValue()){
			tmp = DateUtils.getYears(startTime, endTime);
		}
		return tmp;
	}
	
	/**
	 * 转换为数据库中存储的时间格式
	 * @param timeType
	 * @param time
	 * @return
	 */
	public static String parseTime(int timeType, String time){
		if(timeType == DateType.WEEK.getValue()){
			return DateUtils.getWeekByDay(time);
		}
		return time;
	}
	
	
	/**
	 * time: YYYYMMDD
	 * @param time
	 * @return
	 */
	public static boolean isRealTime(int timeType, String time){
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(Calendar.MONDAY);//每周以周一开始
		cal.setTimeInMillis(System.currentTimeMillis());
		int month = cal.get(Calendar.MONTH) + 1;
		int day =  cal.get(Calendar.DAY_OF_MONTH);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		int year = cal.get(Calendar.YEAR);
		
		if(timeType == DateType.DAY.getValue()){
			String date = year + StringUtil.fillLeftData(month+"", 2, '0') + StringUtil.fillLeftData(day+"" , 2 ,'0');
			if(time.equals(date))
				return true;
		} else if(timeType == DateType.WEEK.getValue()){
			String date = year + StringUtil.fillLeftData(week+"", 2, '0');
			if(time.equals(date))
				return true;
		} else if(timeType == DateType.MONTH.getValue()){
			String date = year + StringUtil.fillLeftData(month+"", 2, '0');
			if(time.equals(date))
				return true;
		} else if(timeType == DateType.YEAR.getValue()){
			if(time.equals(year + ""))
				return true;
		}
		return false;
	}
	
	/**
	 * 获取本周今天之前日期集合（包括今日）
	 * @param time YYYYMMDD
	 * @return
	 */
	public static List<String> getDaysForThisWeek(){
		List<String> times = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
	
		int totayDay = cal.get(Calendar.DAY_OF_WEEK);
		if(totayDay == Calendar.SUNDAY)
			totayDay = Calendar.SATURDAY + 1;
		
		for(int day = Calendar.MONDAY; day <= totayDay; day++){
			cal.set(Calendar.DAY_OF_WEEK, day);
			times.add(cal.get(Calendar.YEAR) + StringUtil.fillLeftData(cal.get(Calendar.MONTH) + 1+"", 2, '0') + StringUtil.fillLeftData(cal.get(Calendar.DAY_OF_MONTH)+"" , 2 ,'0'));
		}
		return times;
	}
	
	/**
	 * 获取本月本周之前周集合（包括本周）
	 * @param month YYYYMM
	 * @return
	 */
	public static List<String> getWeeksForThisMonth(){
		List<String> times = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		int thisWeek = cal.get(Calendar.WEEK_OF_YEAR);
		//月是从0开始的， 0 - 11
		cal.set(Calendar.DAY_OF_MONTH, 1);
		
		for(int startWeek = cal.get(Calendar.WEEK_OF_YEAR); startWeek <= thisWeek; startWeek++){
			times.add(cal.get(Calendar.YEAR) + StringUtil.fillLeftData(startWeek+"", 2, '0'));
		}
		return times;
	}

	/**
	 * 获取今年本月之前月集合（包括本月）
	 * @param month YYYY
	 * @return
	 */
	public static List<String> getMonthsForThisYear(){
		List<String> times = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		
		int thisMonth = cal.get(Calendar.MONTH);
		//月是从0开始的， 0 - 11
		for(int startMonth = Calendar.JANUARY; startMonth <= thisMonth; startMonth++){
			times.add(cal.get(Calendar.YEAR) + StringUtil.fillLeftData(startMonth + 1 +"", 2, '0'));
		}
		return times;
	}
	
	/**
	 * 获取@{code timeType}下时间
	 */
	public static List<String> getThisTimes(int timeType, String time){
		if(timeType == DateType.WEEK.getValue()){
			return TimeUtils.getDaysForThisWeek();
		} else if(timeType == DateType.MONTH.getValue()){
			return TimeUtils.getWeeksForThisMonth();
		} else if(timeType == DateType.YEAR.getValue()){
			return TimeUtils.getMonthsForThisYear();
		}
		return Lists.newArrayList(time);
	}
	
	/**
	 * 获取 @{code timeType}下一级
	 * @param timeType
	 * @return
	 */
	public static int getSubTimeType(int timeType){
		if(timeType == DateType.WEEK.getValue()){
			return DateType.DAY.getValue();
		} else if(timeType == DateType.MONTH.getValue()){
			return DateType.WEEK.getValue();
		} else if(timeType == DateType.YEAR.getValue()){
			return DateType.MONTH.getValue();
		}
		return DateType.DAY.getValue();
	}
	
	public static void main(String[] args) {
//		System.out.println(applyDescForTime(DateType.WEEK.getValue(), "201423"));
//		System.out.println(getRecentThrityMinuteTime(System.currentTimeMillis()));
//		System.out.println(getDaysForThisWeek());
//		System.out.println(getWeeksForThisMonth());
//		System.out.println(getMonthsForThisYear());
		System.out.println(parseTimes(1, "20140902", "20140919"));
	}
}
