package com.tracker.common.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class DateUtils {
	
	/**
	 * 获得当天日期
	 */
	public static String getToday() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		return sdf.format(cal.getTime());
	}
	
	/**
	 * 根据起始时间获取日期（天）集合
	 * @param startDay {YYYYMMDD}
	 * @param endDay {YYYYMMDD}
	 * @return
	 */
	public static List<String> getDays(String startDate, String endDate){
		List<String> days = new ArrayList<String>();
		
		if(startDate.length() != 8 || endDate.length() != 8)
			return days;
		
		Calendar cal = Calendar.getInstance();
		//月是从0开始的， 0 - 11
		cal.set( Integer.parseInt(startDate.substring(0, 4)), Integer.parseInt(startDate.substring(4,6)) - 1, Integer.parseInt(startDate.substring(6,8)));
		
		Calendar endCal = Calendar.getInstance();
		endCal.set(Integer.parseInt(endDate.substring(0, 4)), Integer.parseInt(endDate.substring(4, 6)) - 1, Integer.parseInt(endDate.substring(6, 8)));
		
		while(endCal.getTimeInMillis() >= cal.getTimeInMillis()){
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			int day = cal.get(Calendar.DAY_OF_MONTH);
			days.add(year + "" + StringUtil.fillLeftData(String.valueOf(month),2,'0') + "" + StringUtil.fillLeftData(String.valueOf(day),2,'0'));
			cal.setTimeInMillis(cal.getTimeInMillis() + 24*3600*1000L);
		}
		return days;
	}
	
	public static String[] getStartAndEndDayByWeek(int year, int weekOfYear){
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.WEEK_OF_YEAR, weekOfYear);
		cal.set(Calendar.DAY_OF_WEEK, 2);//星期一
		String startDay = applySplitForDate(cal.get(Calendar.YEAR) + "",(cal.get(Calendar.MONTH) + 1) + "",cal.get(Calendar.DAY_OF_MONTH) + "");
		
		cal.setTimeInMillis(cal.getTimeInMillis() + 6 * 24 * 3600 * 1000L);
		String endDay = applySplitForDate(cal.get(Calendar.YEAR) + "",(cal.get(Calendar.MONTH) + 1) + "",cal.get(Calendar.DAY_OF_MONTH) + "");
		
		return new String[]{startDay, endDay};
	}
	
	
	public static String applySplitForDate(String year, String month, String day){
		return year + "/" + month + "/" + day;
	}
	
	
	public static String getWeekByDay(String date){
		if(date.length() != 8)
			return date;
		Calendar cal = Calendar.getInstance();
		//月是从0开始的， 0 - 11
		int year = Integer.parseInt(date.substring(0, 4));
		cal.set( year, Integer.parseInt(date.substring(4,6)) - 1, Integer.parseInt(date.substring(6,8)));
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		//年中第几周
		return year + StringUtil.fillLeftData(String.valueOf(weekOfYear), 2, '0');
	}
	
	public static String getTimeByDay(long millis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(millis);
		String date = cal.get(Calendar.YEAR) + StringUtil.fillLeftData(cal.get(Calendar.MONTH) + 1)
				+ StringUtil.fillLeftData(cal.get(Calendar.DAY_OF_MONTH)) + StringUtil.fillLeftData(cal.get(Calendar.HOUR_OF_DAY));
		return date;
	}
	
	public static int getTime(long millis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(millis);
		return cal.get(Calendar.HOUR_OF_DAY);
	}
	
	public static String getDay(long millis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(millis);
		String date = cal.get(Calendar.YEAR) + StringUtil.fillLeftData(cal.get(Calendar.MONTH) + 1)
				+ StringUtil.fillLeftData(cal.get(Calendar.DAY_OF_MONTH));
		return date;
	}
	
	/**
	 * 获取下一天凌晨
	 * @return
	 */
	public static long getNextDay(){
		Calendar cal = Calendar.getInstance(); 
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.DAY_OF_MONTH, 1); 
        return cal.getTimeInMillis();
	}
	
	public static long getTodayZeroHour(){
		Calendar cal = Calendar.getInstance(); 
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
	}
	
	public static boolean isSameDay(Date date1, Date date2) {
		if (date1 == null || date2 == null) {
			throw new IllegalArgumentException("The date must not be null");
		}
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(date1);
		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(date2);
		return isSameDay(cal1, cal2);
	}

	public static boolean isSameDay(Calendar cal1, Calendar cal2) {
		if (cal1 == null || cal2 == null) {
			throw new IllegalArgumentException("The date must not be null");
		}
		return (cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) && cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) && cal1
				.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR));
	}

	/**
	 * 根据起始时间获取周集合
	 * @param startDay {YYYYMMDD}
	 * @param endDay {YYYYMMDD}
	 * @return {YYYYWW}
	 */
	public static List<String> getWeeks(String startDate, String endDate){
		List<String> weeks = new ArrayList<String>();
		
		if(startDate.length() != 8 || endDate.length() != 8)
			return weeks;
		
		Calendar cal = Calendar.getInstance();
		//月是从0开始的， 0 - 11
		cal.set( Integer.parseInt(startDate.substring(0, 4)), Integer.parseInt(startDate.substring(4,6)) - 1, Integer.parseInt(startDate.substring(6,8)));
		cal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		cal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		
		Calendar endCal = Calendar.getInstance();
		endCal.set(Integer.parseInt(endDate.substring(0, 4)), Integer.parseInt(endDate.substring(4, 6)) - 1, Integer.parseInt(endDate.substring(6, 8)));
		endCal.setFirstDayOfWeek(GregorianCalendar.MONDAY);//每周以周一开始
		endCal.setMinimalDaysInFirstWeek(3);//每年的第一周必须大于或等于3天，否则就算上一年的最后一周
		
		int lastWeekOfYear = 0;
		while(endCal.getTimeInMillis() >= cal.getTimeInMillis()){
			int year = cal.get(Calendar.YEAR);
			int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
			lastWeekOfYear = weekOfYear;
			//年中第几周
			weeks.add(year + StringUtil.fillLeftData(String.valueOf(weekOfYear), 2, '0'));
			cal.setTimeInMillis(cal.getTimeInMillis() + 7*24*3600*1000L);
		}
		int endWeekOfYear = endCal.get(Calendar.WEEK_OF_YEAR);
		if(endWeekOfYear != lastWeekOfYear)
			weeks.add(endCal.get(Calendar.YEAR) + StringUtil.fillLeftData(String.valueOf(endWeekOfYear), 2, '0'));
		
		return weeks;
	}
	
	/**
	 * 根据起始时间获取月集合
	 * @param startDay {YYYYMM}
	 * @param endDay {YYYYMM}
	 * @return {YYYYMM}
	 */
	public static List<String> getMonths(String startDate, String endDate){
		List<String> months = new ArrayList<String>();
		
		if(startDate.length() != 6 || endDate.length() != 6)
			return months;
		
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, Integer.parseInt(startDate.substring(0, 4)));
		cal.set(Calendar.MONTH, Integer.parseInt(startDate.substring(4,6)) - 1);//月是从0开始的， 0 - 11
		
		Calendar endCal = Calendar.getInstance();
		endCal.set(Calendar.YEAR, Integer.parseInt(endDate.substring(0, 4)));
		endCal.set(Calendar.MONTH, Integer.parseInt(endDate.substring(4,6)) - 1);//月是从0开始的， 0 - 11
		
		while(endCal.getTimeInMillis() >= cal.getTimeInMillis()){
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			months.add(year + StringUtil.fillLeftData(String.valueOf(month), 2, '0'));
			cal.setTimeInMillis(cal.getTimeInMillis() + getDaysOfMonth(year, month) * 24 * 3600 * 1000L);
		}
		return months;
	}
	
	/**
	 * 根据起始时间获取月集合
	 * @param startDay {YYYY}
	 * @param endDay {YYYY}
	 * @return {YYYY}
	 */
	public static List<String> getYears(String startDate, String endDate){
		List<String> years = new ArrayList<String>();
		
		if(startDate.length() != 4 || endDate.length() != 4)
			return years;
		
		int startYear = Integer.parseInt(startDate);
		int endYear = Integer.parseInt(endDate);
		
		while(endYear >= startYear){
			years.add(String.valueOf(startYear));
			startYear++;
		}
		return years;
	}
	
	public static int getDaysOfMonth(int year, int month) {
		switch (month) {
			case 4:		
			case 6:		
			case 9:
			case 11:
				return 30;
			case 2:
				if (isLeapYear(year)) {
					return 29;
				} else {
					return 28;
				}
			default:
				return 31;
		}
	}
	
	public static boolean isLeapYear(int year) {
		if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0) {
			return true;
		}
		return false;
	}
	
	public static boolean isNewVisitor(long cookieCreateTime, long serverTime) {
		return getDay(cookieCreateTime * 1000).equals(getDay(serverTime));
	}
	
	public static void main(String[] args) {
//		System.out.println(getWeeks("20141228", "20150108"));
//		System.out.println(getMonths("201310", "201406"));
//		System.out.println(getYears("2013", "2015"));
		
//		System.out.println(Arrays.asList(getStartAndEndDayByWeek(2014, 25)));
//		System.out.println(getTimeByDay(System.currentTimeMillis()));
		System.out.println(getDay(1410234278000L));
		
//		System.out.println(getNextDay());
//		System.out.println(System.currentTimeMillis());
		
	}
}
