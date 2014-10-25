package com.tracker.common.constant.search;

import java.util.ArrayList;
import java.util.List;

/**
 * 搜索结果数量类型
 * @author jason.hua
 *
 */
public enum SearchResultType {

	DISPLAY_PAGE_NUM(1, "展示页码"),
	SEARCH_RESULT_COUNT(2, "搜索结果数"),
	SEARCH_COST(3, "搜索耗时"),
	SEARCH_TIME(4, "搜索时段");
	
	private final int type;
	private final String desc;
	
	private SearchResultType(int type, String desc){
		this.type = type;
		this.desc = desc;
	}

	public int getType() {
		return type;
	}

	public String getDesc() {
		return desc;
	}
	
	public static SearchResultType valueOf(int value){
		switch(value){
			case 1:
				return SearchResultType.DISPLAY_PAGE_NUM;
			case 2:
				return SearchResultType.SEARCH_RESULT_COUNT;
			case 3:
				return SearchResultType.SEARCH_COST;
			case 4:
				return SearchResultType.SEARCH_TIME;
			default:
				return null;
		}
	}
	
	public static List<Integer> getResultTypeValues(int resultType){
		List<Integer> list = new ArrayList<Integer>();
		switch(resultType){
			case 1:
				SearchPageNumType[] types = SearchPageNumType.values();
				for(SearchPageNumType type: types){
					list.add(type.getType());
				}
				return list;
			case 2:
				SearchResultCountType[] countTypes = SearchResultCountType.values();
				for(SearchResultCountType type: countTypes){
					list.add(type.getType());
				}
				return list;
			case 3:
				SearchCostType[] costTypes = SearchCostType.values();
				for(SearchCostType type: costTypes){
					list.add(type.getType());
				}
				return list;
			case 4:
				for(int time = 0; time <= 23; time++){
					list.add(time);
				}
				return list;
			default:
				return list;
		}
	}
}
