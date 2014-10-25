 package com.tracker.common.constant.search;

/**
 * 搜索耗时类型
 * @author jason.hua
 *
 */
public enum SearchCostType {

	FIVE_HUN_MILLS(1, "500毫秒以内"),
	FIVE_ONE_SEC(2, "500毫秒 - 1秒"),
	ONE_TWO(3, "1-2秒"),
	TWO_FIVE(4, "2-5秒"),
	FIVE_ABOVE(5, "5秒以上");
	
	private final int type;
	private final String desc;
	
	private SearchCostType(int type, String desc){
		this.type = type;
		this.desc = desc;
	}

	public int getType() {
		return type;
	}


	public String getDesc() {
		return desc;
	}
	
	public static SearchCostType valueOf(int value){
		switch(value){
			case 1:
				return SearchCostType.FIVE_HUN_MILLS;
			case 2:
				return SearchCostType.FIVE_ONE_SEC;
			case 3:
				return SearchCostType.ONE_TWO;
			case 4:
				return SearchCostType.TWO_FIVE;
			case 5:
				return SearchCostType.FIVE_ABOVE;
			default:
				return null;
		}
	}
	
	/**
	 * 获取SearchCostType
	 * @param cost 毫秒级别
	 * @return
	 */
	public static SearchCostType getType(int cost){
		if (cost <= 500)
			return SearchCostType.FIVE_HUN_MILLS;
		else if (cost <= 1000)
			return SearchCostType.FIVE_ONE_SEC;
		else if (cost <= 2000)
			return SearchCostType.ONE_TWO;
		else if (cost <= 5000)
			return SearchCostType.TWO_FIVE;
		else if (cost > 5000)
			return SearchCostType.FIVE_ABOVE;
		return null;
	}
}
