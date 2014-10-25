package com.tracker.db.constants;

public enum AreaLevel {
	COUNTRY(1), //国家级
	PROVINCE(2), //省级
	CITY(3); //市级

	private final int value;

	private AreaLevel(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	/**
	 * Find a the enum type by its integer value, as defined in the Thrift IDL.
	 * 
	 * @return null if the value is not found.
	 */
	public static AreaLevel findByValue(int value) {
		switch (value) {
		case 1:
			return COUNTRY;
		case 2:
			return PROVINCE;
		case 3:
			return PROVINCE;
		default:
			return null;
		}
	}
}
