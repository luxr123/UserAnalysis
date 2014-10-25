package com.tracker.hive.udf;

import com.tracker.db.constants.DateType;

/**
 * udf 工具类
 * @author xiaorui.lu
 * 
 */
public class UDFUtils {

	/**
	 * 函数名：getDateType
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-23 下午2:36:29
	 * 功能描述：日期类型转换(0:day 1:week 2:month, 3:year)
	 * @param dateType
	 * @return
	 */
	public static int getDateType(int dateType) {
		int rowDateType = -1;
		
		switch (dateType) {
			case 0:
				rowDateType = DateType.DAY.getValue();
				break;
			case 1:
				rowDateType = DateType.WEEK.getValue();
				break;
			case 2:
				rowDateType = DateType.MONTH.getValue();
				break;
			case 3:
				rowDateType = DateType.YEAR.getValue();
				break;
			default:
				rowDateType = dateType;
		}
		
		return rowDateType;
	}
}
