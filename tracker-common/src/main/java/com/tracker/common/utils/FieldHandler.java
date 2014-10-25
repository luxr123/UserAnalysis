package com.tracker.common.utils;


/**
 * Convert java type object to hbase's bytes back and forth.
 * support type: String, Boolean, Long, Integer, Double
 * 
 * */
public class FieldHandler {

    public static Object stringToObject(Class<?> type, String value){
    	if(type == String.class){
    		return value;
    	} else if(type == Boolean.class){
    		return Boolean.valueOf(value.trim());
    	} else if(type == Long.class){
    		return Long.parseLong(value.trim());
    	} else if(type == Integer.class){
    		return Integer.valueOf(value.trim());
    	} else if(type == Double.class){
    		return Double.valueOf(value.trim());
    	}
    	return value;
    }
    
    public static void main(String[] args) {
		System.out.println(stringToObject(Boolean.class, "true"));
	}
}
