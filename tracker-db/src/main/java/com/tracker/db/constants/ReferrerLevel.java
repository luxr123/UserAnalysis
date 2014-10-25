package com.tracker.db.constants;

public enum ReferrerLevel {
	DOMAIN_LEVEL(1), //主域名
	SUB_DOMAIN_LEVEL(2); //子域名

	private final int value;

	private ReferrerLevel(int value) {
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
	public static ReferrerLevel findByValue(int value) {
		switch (value) {
		case 1:
			return DOMAIN_LEVEL;
		case 2:
			return SUB_DOMAIN_LEVEL;
		default :
			return null;
		}
	}
}
