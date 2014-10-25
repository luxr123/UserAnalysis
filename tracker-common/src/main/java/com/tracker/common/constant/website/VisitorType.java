package com.tracker.common.constant.website;

public enum VisitorType {
	NEW_VISITOR(1, "新访客"), 
	OLD_VISITOR(2, "老访客");

	private final int value;
	private final String desc;

	private VisitorType(int value, String desc) {
		this.value = value;
		this.desc = desc;
	}

	public int getValue() {
		return value;
	}

	public String getDesc() {
		return desc;
	}

	/**
	 * Find a the enum type by its integer value, as defined in the Thrift IDL.
	 * 
	 * @return null if the value is not found.
	 */
	public static VisitorType findByValue(int value) {
		switch (value) {
		case 1:
			return NEW_VISITOR;
		case 2:
			return OLD_VISITOR;
		default:
			return null;
		}
	}

}
