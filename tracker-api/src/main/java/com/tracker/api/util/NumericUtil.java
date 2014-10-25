package com.tracker.api.util;

import com.tracker.common.utils.StringUtil;

/**
 * 数值工具类
 * @author jason.hua
 *
 */
public class NumericUtil {
	private static java.text.DecimalFormat   df=new   java.text.DecimalFormat("0.00");
	
	/**
	 * 两个数相除，并保留小数点后两位数值
	 * @param firstValue
	 * @param secondValue
	 * @return
	 */
	public static String getRate(Long firstValue, Long secondValue){
		if(firstValue == null || secondValue == null || secondValue == 0)
			return "0";
		String result = df.format((double)firstValue / secondValue);
		if(result.indexOf(".00") > -1)
			result = result.replace(".00", "");
		return result;
	}
	
	public static String getRateForPCT(Long firstValue, Long secondValue){
		if(firstValue == null || secondValue == null || secondValue == 0)
			return "0%";
		String result = df.format(100 * (double)firstValue / secondValue);
		if(result.indexOf(".00") > -1)
			result = result.replace(".00", "");
		return result + "%";
	}
//	
//	public static String getRateForPCT(double value){
//		String result = df.format(100 * value);
//		if(result.indexOf(".00") > -1)
//			result = result.replace(".00", "");
//		return result + "%";
//	}
//	
//	public static double parseRateFromPCT(String value){
//		value = value.replace("%", "");
//		return Double.parseDouble(value) / 100;
//	}
//	
//	public static String getAvgTime(Long firstValue, Long secondValue){
//		if(firstValue == null || secondValue == null || secondValue == 0)
//			return "00:00:00";
//		long value = firstValue / secondValue;
//		
//		String second = StringUtil.fillLeftData(String.valueOf(value % 60), 2, '0');
//		String minute = StringUtil.fillLeftData(String.valueOf(value / 60 % 60), 2, '0');
//		String hour = StringUtil.fillLeftData(String.valueOf(value / 3600), 2, '0');
//		return hour + ":" + minute + ":" + second;
//	}
//	
//	public static long parseAvgTime(String time){
//		String[] data = time.split(":");
//		if(data == null || data.length != 3)
//			return 0;
//		
//		int hour = Integer.parseInt(data[0]);
//		int minute = Integer.parseInt(data[1]);
//		int second = Integer.parseInt(data[2]);
//		return hour * 3600 + minute * 60 + second;
//	}
	
	public static int getAvgTimeForMilliSec(Long firstValue, Long secondValue){
		if(firstValue == null || secondValue == null || secondValue == 0)
			return 0;
		return (int) (firstValue / secondValue);
	}
	
	/**
	 * 两个数相除，并保留小数点后两位数值
	 * @param firstValue
	 * @param secondValue
	 * @return
	 */
	public static double getDoubleRate(Long firstValue, Long secondValue){
		if(firstValue == null || secondValue == null || secondValue == 0)
			return 0.0;
		String result = df.format((double)firstValue / secondValue);
		return Double.parseDouble(result);
	}
	
	public static void main(String[] args) {
	}
}
