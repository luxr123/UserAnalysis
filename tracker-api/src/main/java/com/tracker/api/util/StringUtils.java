package com.tracker.api.util;

public class StringUtils {
	public static String removeAreaSuffix(String area){
		if(area.indexOf("省") > -1 || area.indexOf("市") > -1){
			area = area.substring(0, area.length() - 1);
		}
		return area;
	}
}
