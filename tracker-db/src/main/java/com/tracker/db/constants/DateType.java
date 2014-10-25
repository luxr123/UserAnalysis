package com.tracker.db.constants;

public enum DateType {
	TIME(0), DAY(1), WEEK(2), MONTH(3), YEAR(4);

	private final int value;

	private DateType(int value) {
		this.value = value;
	}

	/**
	 * Get the integer value of this enum value, as defined in the Thrift IDL.
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Find a the enum type by its integer value, as defined in the Thrift IDL.
	 * 
	 * @return null if the value is not found.
	 */
	public static DateType findByValue(int value) {
		switch (value) {
		case 1:
			return DAY;
		case 2:
			return WEEK;
		case 3:
			return MONTH;
		case 4:
			return YEAR;
		default:
			return null;
		}
	}
}
