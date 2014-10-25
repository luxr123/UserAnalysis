package com.tracker.data.date;

public class DateUtil {
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
}
