package com.tracker.common.utils;

public class NumericUtil {
	public static Long addValue(Long firstVal, Long secondVal){
		if(firstVal == null && secondVal == null)
			return null;
		
		if(firstVal == null)
			return secondVal;
		
		if(secondVal == null)
			return firstVal;
		
		return firstVal + secondVal;  
	}
	
	
	public static void main(String[] args) {
		Long firstVal = null;
		Long secondVal = 2L;
		Long value = addValue(firstVal, secondVal);
		
		secondVal = 4L;
		System.out.println(value);
		secondVal = 4L;
		System.out.println(value);
	}
}
