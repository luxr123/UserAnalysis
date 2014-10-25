namespace java com.tracker.api.thrift.search
namespace php SearchStatsService

/**
 * 站内搜索结果
 */
struct SearchStats {
	1: optional string name, //类型值
	2: optional string date, //时间
	3: optional i32 fieldId, //展示名所属id
    4: optional i64 searchCount, //搜索次数
    5: optional i64 searchUserCount, //搜索人数
    6: optional i64 ipCount, //IP数
	7: optional string searchCountRate, //搜索次数占比
	8: optional i32 avgSearchCost, //平均搜索耗时，精确到毫秒
	9: optional i64 maxSearchCost, //最大搜索耗时
	10: optional i64 pageTurningCount, // 翻页次数
	11: optional i64 mainPageCount //首页次数
}

struct SearchStatsResult {
	1: required list<SearchStats> statsList, //统计数据集合
    2: required i64  totalCount //结果数量
}

struct SearchEngineParam{
	1: optional i32 searchEngineId, //搜索引擎id
	2: optional i32 searchType
}

struct TopResponseTimeStats{
	1: optional i32 userType, //用户类型值
    2: optional string userTypeName, //用户类型，匿名用户、经理人、猎头
    3: optional i32 userId, //用户id
    4: optional string ip, //ip
    5: optional string cookieId, //用户标识码
	6: optional string visitTime, //搜索时间
	7: optional i32 responseTime, //响应时间
	8: optional i64 totalResultCount, //总的搜索结果数量
	9: optional string searchValueStr //查询值
}

struct TopResponseTimeResult{
	1: required list<TopResponseTimeStats> statsList, //统计数据集合
	2: required i64  totalCount //结果数量
}

/**
 * 搜索行为分析数据服务
 */
service SearchStatsService {
	
	/**
     * 获取实时统计数据
     */	
	map<string, SearchStats>  getSearchStats(1: i32 webId, 2: i32 timeType, 3: string time);

	/**
     * 获取总搜索次数
     */	
	i64 getTotalSearchCount(1: i32 webId, 2: i32 timeType, 3: string time, 4: SearchEngineParam seParam);
	
	/**
	 * 获取基于事件的统计数据
	 */
	list<SearchStats> getSearchStatsForDate(1: i32 webId, 2: i32 timeType, 3: list<string> times, 4: SearchEngineParam seParam);
	
	/**
	 * 获取搜索结果/搜索条件统计数据
	 */
	list<SearchStats> getSiteSearchStats(1: i32 webId, 2: i32 timeType, 3: string time, 4: SearchEngineParam seParam, 5:i32 resultType);
	
	/**
	 * 获取基于页面的统计数据
	 */
	list<SearchStats> getSearchPageStats(1: i32 webId, 2: i32 timeType, 3: string time, 4: SearchEngineParam seParam);
	
	/**
	 * top搜索值
	 */
	SearchStatsResult getSearchValueStats(1: i32 webId, 2: i32 timeType, 3: string time, 4:SearchEngineParam seParam, 5:i32 conditionType, 6: i32 startIndex,7: i32 offset);
	
	/**
	 * top响应时间最慢
	 */ 
	TopResponseTimeResult getTopResponseTimeResult(1: i32 webId, 2: i32 timeType, 3: string time, 4:SearchEngineParam seParam, 5: i32 startIndex,6: i32 offset);

	/**
	 * top ip 
	 */ 
	SearchStatsResult getTopIpResult(1: i32 webId, 2: i32 timeType, 3: string time, 4:SearchEngineParam seParam, 5: i32 startIndex, 6: i32 offset);	
}
