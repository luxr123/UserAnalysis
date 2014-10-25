package com.tracker.hbase.service.run;

/**
 * 文件名：Business
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午3:11:54
 * 功能描述：枚举业务名称，便于在Count中使用switch结构
 */
public enum Business {
	countWebUserStats,
	countWebStatsByHour,
	countWebStats,
	countWebStatsForRefType,
	countWebStatsForProvince,
	countWebStatsForCity,
	countWebStatsForPage,
	countWebStatsForEntryPage,
	countWebStatsForSysEnv,
	countSearchStats,
	countSearchResultStats,
	countSearchConditionStats;
 
	/**
	 * 函数名：getBusiness
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-23 下午4:39:01
	 * 功能描述：返回名称为business的枚举常量
	 * @param business
	 * @return
	 */
	public static Business getBusiness(String business) {
		return valueOf(business);
	} 
}
