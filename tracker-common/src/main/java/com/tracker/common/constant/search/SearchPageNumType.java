package com.tracker.common.constant.search;


/**
 * 展示页类型
 * @author jason.hua
 *
 */
public enum SearchPageNumType {

	ONE(1, "第1页"),
	TWO(2, "第2页"),
	THREE(3, "第3页"),
	FOUR(4, "第4页"),
	FIVE(5, "第5页"),
	SIX(6, "第6页"),
	SEVEN(7, "第7页"),
	EIGHT(8, "第8页"),
	NINE(9, "第9页"),
	TEN(10, "第10页"),
	ELEVEN_FIFTEEN(11, "11-15页"),
	SIXTEEN_TWENTY(12, "16-20页"),
	TWENTY_ABOVE(13, "20页以上");
	
	private final int type;
	private final String desc;
	
	private SearchPageNumType(int type, String desc){
		this.type = type;
		this.desc = desc;
	}

	public int getType() {
		return type;
	}

	public String getDesc() {
		return desc;
	}
	
	public static SearchPageNumType valueOf(int value){
		switch(value){
			case 1:
				return SearchPageNumType.ONE;
			case 2:
				return SearchPageNumType.TWO;
			case 3:
				return SearchPageNumType.THREE;
			case 4:
				return SearchPageNumType.FOUR;
			case 5:
				return SearchPageNumType.FIVE;
			case 6:
				return SearchPageNumType.SIX;
			case 7:
				return SearchPageNumType.SEVEN;
			case 8:
				return SearchPageNumType.EIGHT;
			case 9:
				return SearchPageNumType.NINE;
			case 10:
				return SearchPageNumType.TEN;
			case 11:
				return SearchPageNumType.ELEVEN_FIFTEEN;
			case 12:
				return SearchPageNumType.SIXTEEN_TWENTY;
			case 13:
				return SearchPageNumType.TWENTY_ABOVE;
			default:
				return null;
		}
	}
	
	/**
	 * 获取SearchPageNumType
	 * @param pageNum 展示页数
	 * @return
	 */
	public static SearchPageNumType getType(int pageNum){
		if(pageNum <= 10)
			return valueOf(pageNum);

		if(pageNum <= 15){
			return SearchPageNumType.ELEVEN_FIFTEEN;
		} else if(pageNum <= 20){
			return SearchPageNumType.SIXTEEN_TWENTY;
		} else if(pageNum > 20){
			return SearchPageNumType.TWENTY_ABOVE;
		} 
		return null;
	}
	
}
