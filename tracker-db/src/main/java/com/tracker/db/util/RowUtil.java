package com.tracker.db.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowUtil {
	private static Logger logger = LoggerFactory.getLogger(RowUtil.class);
	
	public static final char  ch = 16;
	public static final String ROW_SPLIT = String.valueOf(ch); // char(16）用于分隔符
	public static final String FIELD_SPLIT = "-"; // char(16）用于分隔符
	public static final char ROW_FILL = '*';
	
	public static String getRowField(String row, int index){
		return getRowField(row, index, ROW_SPLIT);
	}
	
	public static Integer getRowIntField(String row, int index){
		String[] data = row.split(ROW_SPLIT);
		if(data.length <= index)
			return -1;
		try{
			return Integer.parseInt(data[index]);
		} catch(Exception e){
			logger.error("error to getRowIntField, data:" + data[index], e);
		}
		return -1;
	}
	
	public static String getRowField(String row, int index, String split){
		String[] data = row.split(split);
		if(data.length <= index)
			return null;
		return data[index];
	}
	
	public static void main(String[] args) {
//		System.out.println("2_1_1".split(ROW_SPLIT).length);
		System.out.println(String.valueOf(16));
		char ch = 16;
		System.out.println(String.valueOf(ch));
	}
	
}
