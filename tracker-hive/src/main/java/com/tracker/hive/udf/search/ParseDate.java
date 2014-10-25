package com.tracker.hive.udf.search;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.tracker.hive.db.HiveService;

/**
 * 日期解析
 * @author xiaorui.lu
 * 
 */
public class ParseDate extends UDF {
	private static Map<String, Integer> dateTimeCache = HiveService.getDateTimeCache();
	private final static DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	private final static int DEFAULT_DATE_ID = -1;
	
	/**
	 * @param serverTime 时间戳
	 * @return dateId
	 */
	public Integer evaluate(long serverTime) {
		String date = dateFormat.format(new Date(serverTime));
		Integer dateId = dateTimeCache.get(date);
		
		if (dateId == null) {
			return DEFAULT_DATE_ID;
		}
		
		return dateId;
	}
}
