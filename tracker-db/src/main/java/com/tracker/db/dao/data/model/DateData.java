package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 
 * 文件名：DateData
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:07:36
 * 功能描述：日期数据实体类
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class DateData {
	/**
	 * row中各个字段index值
	 */
	public static final int ID_INDEX = 2;
	
	@HBaseColumn(qualifier = "date")
	public String date; //日期, 格式为(YYYYMMDD)
	
	@HBaseColumn(qualifier = "calendar")
	public String calendar; //格式为（YYYYMM）
	
	@HBaseColumn(qualifier = "year")
	public Integer year; //格式为（YYYY）
	
	@HBaseColumn(qualifier = "month")
	public Integer month; //哪个月（1 -12）
	
	@HBaseColumn(qualifier = "dayOfMonth")
	public Integer dayOfMonth; //一月中哪一号
	
	@HBaseColumn(qualifier = "quarterOfYear")
	public Integer quarterOfYear; //一年中哪个季节
	
	@HBaseColumn(qualifier = "dayOfWeek")
	public Integer dayOfWeek; //星期几
	
	@HBaseColumn(qualifier = "weekOfYear")
	public String weekOfYear; //一年中第几个周
	
	@HBaseColumn(qualifier = "isWeekEnd")
	public Boolean  isWeekEnd; //是否是周末

	public DateData(){}
	
	/**
	 * 函数名：generateRowKey
	 * 功能描述：生成rowkey值
	 * @param year 年YYYYMMDD
	 * @param id 日期id
	 * @return
	 */
	public static String generateRowKey(Integer year, Integer id){
		Util.checkZeroValue(id);
		return generateRowPrefix(year) + String.valueOf(id);
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param year 年YYYYMMDD
	 * @return
	 */
	public static String generateRowPrefix(Integer year){
		Util.checkZeroValue(year);
		return DataKeySign.SIGN_DATE + RowUtil.ROW_SPLIT + year + RowUtil.ROW_SPLIT ;
	}

	public String getCalendar() {
		return calendar;
	}

	public void setCalendar(String calendar) {
		this.calendar = calendar;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Integer getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(Integer dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public Integer getQuarterOfYear() {
		return quarterOfYear;
	}

	public void setQuarterOfYear(Integer quarterOfYear) {
		this.quarterOfYear = quarterOfYear;
	}

	public Integer getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(Integer dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public String getWeekOfYear() {
		return weekOfYear;
	}

	public void setWeekOfYear(String weekOfYear) {
		this.weekOfYear = weekOfYear;
	}

	public Boolean getIsWeekEnd() {
		return isWeekEnd;
	}

	public void setIsWeekEnd(Boolean isWeekEnd) {
		this.isWeekEnd = isWeekEnd;
	}

	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}

}
