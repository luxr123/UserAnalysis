package com.tracker.hive.service;

import java.util.Calendar;
import java.util.Map;

import com.tracker.common.constant.website.VisitorType;
import com.tracker.common.utils.StringUtil;
import com.tracker.hive.db.HiveService;

/**
 * 文件名：FieldParser
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-21 下午4:56:16
 * 功能描述：根据输入Field生成所需信息
 *
 */
public class FieldParser {
	private static Map<String, Integer> dateTimeCache = null;
	
	/**
	 * 函数名：parseDate
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 下午4:51:19
	 * 功能描述：将timeMillis转换为dateId和hour组成的数组
	 * @param timeMillis
	 * @return
	 */
	public static Integer[] parseDate(long timeMillis) {
		if(dateTimeCache == null){
			synchronized(FieldParser.class){
				if(dateTimeCache == null){
					dateTimeCache = HiveService.getDateTimeCache();
				}
			}
		}
		
		//设置时间
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timeMillis);
		
		int year = cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH) + 1;
		int day = cal.get(Calendar.DAY_OF_MONTH);
		
		String date = year + StringUtil.fillLeftData(month + "", 2, '0') + StringUtil.fillLeftData(day+"", 2, '0');
		
		
		Integer dateId = dateTimeCache.get(date);
		if(dateId == null) {
			dateId = -1;
		}
		
		Integer time = cal.get(Calendar.HOUR_OF_DAY);
		
		return new Integer[] {dateId, time};
	}
	
	/**
	 * 函数名：parseCookieCreateTime
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 下午4:49:12
	 * 功能描述：根据ckct和visitTime判断访客类型（新/老）
	 * @param ckct
	 * @param visitTime
	 * @return
	 */
	public static int[] parseCookieCreateTime(long ckct, long visitTime){
		int[] visitorType = new int[4];
		
		//设置时间
		Calendar ckctCal = Calendar.getInstance();
		ckctCal.setTimeInMillis(ckct);
		Calendar vtCal = Calendar.getInstance();
		vtCal.setTimeInMillis(visitTime);
		
		//day
		if(ckctCal.get(Calendar.DAY_OF_YEAR) == vtCal.get(Calendar.DAY_OF_YEAR)) {
			visitorType[0] = VisitorType.NEW_VISITOR.getValue();
		} else {
			visitorType[0] = VisitorType.OLD_VISITOR.getValue();
		} 
		
		//week
		if(ckctCal.get(Calendar.WEEK_OF_YEAR) == vtCal.get(Calendar.WEEK_OF_YEAR)) {
			visitorType[1] = VisitorType.NEW_VISITOR.getValue();
		} else {
			visitorType[1] = VisitorType.OLD_VISITOR.getValue();
		} 
		
		//month
		if(ckctCal.get(Calendar.MONTH) == vtCal.get(Calendar.MONTH)) {
			visitorType[2] = VisitorType.NEW_VISITOR.getValue();
		} else {
			visitorType[2] = VisitorType.OLD_VISITOR.getValue();
	    } 
		
		//year
		if(ckctCal.get(Calendar.YEAR) == vtCal.get(Calendar.YEAR)) {
			visitorType[3] = VisitorType.NEW_VISITOR.getValue();
		} else {
			visitorType[3] = VisitorType.OLD_VISITOR.getValue();
		} 
			
		return visitorType;
	}
	
	
	public static void main(String[] args) {
/*		int[] type = parseCookieCreateTime(System.currentTimeMillis(), System.currentTimeMillis() + 24*3600*1000);
		for(int i=0; i < type.length; i++)
			System.out.println(VisitorType.findByValue(type[i]));
//		System.out.println(parseReferrer(2, ""));
		System.out.println(Arrays.asList(parseDate(System.currentTimeMillis())));*/
		
		System.out.println(Calendar.getInstance().get(Calendar.HOUR_OF_DAY));
	}
}
