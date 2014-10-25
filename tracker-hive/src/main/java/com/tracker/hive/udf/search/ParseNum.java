package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.tracker.common.constant.search.SearchCostType;
import com.tracker.common.constant.search.SearchPageNumType;
import com.tracker.common.constant.search.SearchResultCountType;

/**
 * 按搜索条件值进行分配到各个展示类型值
 * @author xiaorui.lu
 * 
 */
public class ParseNum extends UDF {
	
	/**
	 * 函数名：evaluate
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-22 下午2:40:07
	 * 功能描述：根据type和num获取特定类型值   搜索页/搜索时间
	 * @param type
	 * @param num
	 * @return
	 */
	public Integer evaluate(Integer type, Integer num) {
		switch (type) {
			case 1:
				if (num == null) {
					return SearchPageNumType.ONE.getType();
				} else if (num >= 0) {
					return SearchPageNumType.getType(num).getType();
				}
			case 3:
				if (num == null) {
					return SearchCostType.FIVE_HUN_MILLS.getType();
				} else if (num >= 0) {
					return SearchCostType.getType(num).getType();
				}
			default:
				return -1;
		}
	}

	/**
	 * 函数名：evaluate
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-22 下午2:45:47
	 * 功能描述：根据totalCount获取搜索结果数量类型
	 * @param totalCount
	 * @return
	 */
	public Integer evaluate(Long totalCount) {
		if (totalCount == null) {
			return SearchResultCountType.ZERO.getType();
		} else if (totalCount >= 0) {
			return SearchResultCountType.getType(totalCount).getType();
		} else {
			return -1;
		}
	}
}
