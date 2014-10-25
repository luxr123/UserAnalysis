package com.tracker.common.constant.website;

/**
 * 访问来源
 * @author jason.hua
 *
 */
public enum ReferrerType {
	DIRECT(1, "直接访问"), //直接访问
	SEARCH_ENGINE(2, "搜索引擎"),//搜索引擎来源
	OTHER_LINK(3, "外部链接");//其他外部链接
	
	private int value;
	private String name;
	
	private ReferrerType(int value, String name){
		this.value = value;
		this.name = name;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static ReferrerType valueOf(int value){
		switch(value){
			case 3:
				return ReferrerType.OTHER_LINK;
			case 2:
				return ReferrerType.SEARCH_ENGINE;
			case 1:
				return ReferrerType.DIRECT;
			default:
				return null;
		}
	}

	public static void main(String[] args) {
		int refType = 1;
		System.out.println(ReferrerType.DIRECT.getValue() == refType);
	}
}