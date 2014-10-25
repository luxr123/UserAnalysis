package com.tracker.common.constant.search;


/**
 * 搜索结果数量类型
 * @author jason.hua
 *
 */
public enum SearchResultCountType {

	ZERO(1, "0条"),
	ONE_ONE_H(2, "1-100条"),
	ONE_H_THREE_H(3, "100-300条"),
	THREE_H_FIVE_H(4, "300-500条"),
	FIVE_H_ONE_T(5, "500-1000条"),
	ONE_T_THREE_T(6, "1000-3000条"),
	THREE_T_FIVE_T(7, "3000-5000条"),
	FIVE_T_TEN_T(8, "5000-10000条"),
	TEN_T_FIFTH_T(9, "10000-50000条"),
	FIFTH_T_ABOVE(10, "50000条以上");
	
	private final int type;
	private final String desc;
	
	private SearchResultCountType(int type, String desc){
		this.type = type;
		this.desc = desc;
	}


	public int getType() {
		return type;
	}

	public String getDesc() {
		return desc;
	}
	
	public static SearchResultCountType valueOf(int value){
		switch(value){
			case 1:
				return SearchResultCountType.ZERO;
			case 2:
				return SearchResultCountType.ONE_ONE_H;
			case 3:
				return SearchResultCountType.ONE_H_THREE_H;
			case 4:
				return SearchResultCountType.THREE_H_FIVE_H;
			case 5:
				return SearchResultCountType.FIVE_H_ONE_T;
			case 6:
				return SearchResultCountType.ONE_T_THREE_T;
			case 7:
				return SearchResultCountType.THREE_T_FIVE_T;
			case 8:
				return SearchResultCountType.FIVE_T_TEN_T;
			case 9:
				return SearchResultCountType.TEN_T_FIFTH_T;
			case 10:
				return SearchResultCountType.FIFTH_T_ABOVE;
			default:
				return null;
		}
	}
	
	/**
	 * 获取SearchPageNumType
	 * @param resultCount 结果数量
	 * @return
	 */
	public static SearchResultCountType getType(long resultCount){
		if(resultCount == 0){
			return SearchResultCountType.ZERO;
		} else if(resultCount <= 100){
			return SearchResultCountType.ONE_ONE_H;
		} else if(resultCount <= 300){
			return SearchResultCountType.ONE_H_THREE_H;
		} else if(resultCount <= 500){
			return SearchResultCountType.THREE_H_FIVE_H;
		} else if(resultCount <= 1000){
			return SearchResultCountType.FIVE_H_ONE_T;
		} else if(resultCount <= 3000){
			return SearchResultCountType.ONE_T_THREE_T;
		} else if(resultCount <= 5000){
			return SearchResultCountType.THREE_T_FIVE_T;
		}  else if(resultCount <= 10000){
			return SearchResultCountType.FIVE_T_TEN_T;
		}  else if(resultCount <= 50000){
			return SearchResultCountType.TEN_T_FIFTH_T;
		}  else if(resultCount > 50000){
			return SearchResultCountType.FIFTH_T_ABOVE;
		} 
		return null;
	}
}
