namespace java com.tracker.api.thrift.web
namespace php WebStatsService

/**
 * 用户信息
 */
struct UserInfo{
    1: optional string lastVisitTime, //访问时间
    2: optional string ip, //ip
    3: optional string cookieId, //用户标识码
    4: optional i32 userType, //用户类型值
    5: optional string userTypeName, //用户类型，匿名用户、经理人、猎头
    6: optional i32 userId, //用户id
    7: optional string url, //用户当前访问页面
    8: optional i32 totalVisitTime, // 访问时长
    9: optional i32 visitPages //访问页面数
}

/**
 * 访客集合
 */
struct UserInfoResult{
	1: required list<UserInfo> userInfoList,
	2: required i32 totalCount
}

/**
 * 用户访问记录
 */
struct UserLog{
    1: optional string visitTime, //访问时间
    2: optional string visitTypeName, //访问类型，浏览、经理人搜索。。。
    3: optional string ip, //ip
    4: optional string cookieId, //用户标识码
    5: optional i32 userType, //用户类型值
    6: optional string userTypeName, //用户类型，匿名用户、经理人、猎头
    7: optional i32 userId, //用户id
    8: optional string urlOrSearchValue, //url或者搜索条件值
    9: optional i32 responseTime, //搜索响应时间
	10: optional i64 totalResultCount, //搜索结果数量
	11: optional string searchParam //搜索查询参数
}

/**
 * 用户历史记录集合
 */
struct UserLogResult{
	1: required list<UserLog> userLogList,
	2: required i32 totalCount
}

/**
 * 访客类型:新访客(1)、老访客(2)
 */
struct UserFilter{
	1: optional i32 visitorType,  //新访客(1)、老访客(2)
	2: optional i32 userType, //用户类型，经理人、猎头
	3: optional string userId, //用户id
	4: optional string cookieId, //用户cookieId
	5: optional string ip
}

/**
 * 来源类型过滤
 */
struct  ReferrerFilter {
	1: optional i32 refType, //来源类型id
	2: optional i32 seDomainId // 搜索引擎域名id
}

struct LogFilter {
	1: optional i32 visitType, //访问类型(浏览、搜索精英、搜索部门、搜索case)
	2: optional i32 isCallSELog //是否是调用搜索引擎的日志,1：是， 0：不是
}

/**
 * 地域过滤
 */	
struct AreaFilter {
	1: optional i32 countryId, //国家id
	2: optional i32 provinceId //省id
}


/**
  * 网站统计度量指标
  */
struct WebStats {
	1: optional string times, // 时间参数
    2: optional string showName, //展示名
    3: optional i32 fieldId, //展示名所属id，待用
	4: optional i64 pv,//浏览量
	5: optional i64 uv,//访客数
	6: optional i64 ipCount,//ip数
	7: optional i64 visitTimes,//访问次数
	8: optional string jumpRate,//跳出率
	9: optional string avgVisitTime,//平均访问时间
	10: optional string avgVisitPage,//平均访问页数
	11: optional string pvRate//浏览量占比
}

struct PageStats {
	1: optional string times, //时间参数
    2: optional string pageUrl, //页面url/前缀
    3: optional string pageDesc, //页面描述
	4: optional i64 pv,
	5: optional i64 uv,
	6: optional i64 ipCount,
	7: optional i64 entryPageCount, //入口页次数
	8: optional i64 nextPageCount,//贡献下游浏览次数
	9: optional i64 outPageCount// 退出页次数
	10: optional string outRate, //退出率
	11: optional string avgStayTime, //平均停留时长
	12: optional string pvRate//浏览量占比
}

struct EntryPageStats {
	1: optional string times, //时间参数
    2: optional string pageUrl, //页面url/前缀
    3: optional string pageDesc, //页面描述
	4: optional i64 pv,//浏览量
	5: optional i64 uv,//访客数
	6: optional i64 ipCount,//ip数
	7: optional i64 visitTimes,//访问次数
	8: optional string jumpRate,//跳出率
	9: optional string avgVisitTime,//平均访问时间
	10: optional string avgVisitPage,//平均访问页数 
	11: optional string pvRate//浏览量占比
}


/**
 * 网站统计数据服务
 */
service WebStatsService {
	/**
	 * 获取首页统计指标
	 */
	map<string, WebStats> getWebSiteStats(1: i32 webId, 2: i32 timeType, 3: string time);
	
	/**
	 * 获取访问用户信息
	 */
	UserInfoResult getUserInfos(1: i32 webId, 2: i32 timeType, 3: string time, 4:UserFilter userFilter, 5: i32 startIndex, 6: i32 offset);
	
	/**
	 * 获取用户历史记录
	 */
	UserLogResult getUserLog(1: i32 webId, 2: i32 timeType, 3: string time, 4:UserFilter userFilter, 5: LogFilter logFilter, 6: i32 startIndex, 7: i32 offset);
	
	/**
	 * 获取基于时间的统计指标
	 */
	list<WebStats> getWebStatsForDate(1: i32 webId, 2: i32 timeType, 3: list<string> times, 4: UserFilter userFilter);

    /**
     * 获取基于访问来源的统计指标
     */
	list<WebStats> getWebStatsForReferrer(1: i32 webId, 2: i32 timeType, 3: string time, 4: ReferrerFilter refFilter, 5: UserFilter userFilter, 6: i32 startIndex,7: i32 offset);
	
	 /**
     * 获取基于关键词的统计指标
     */
	list<WebStats> getWebStatsForKeyword(1: i32 webId, 2: i32 timeType, 3: string time, 4: i32 seDomainId, 5: UserFilter userFilter, 6: i32 startIndex, 7: i32 offset);
	
	 /**
     * 获取基于地域的统计指标
     */
	list<WebStats> getWebStatsForArea(1: i32 webId, 2: i32 timeType, 3: string time, 4: UserFilter userFilter, 5: AreaFilter areaFilter);
	
	/**
     * 获取基于访问页面的统计指标
     */
	list<PageStats> getWebStatsForPage(1: i32 webId, 2: i32 timeType, 3: string time, 4: UserFilter userFilter);
	
	/**
     * 获取基于入口页面的统计指标
     */
	list<EntryPageStats> getWebStatsForEntryPage(1: i32 webId, 2: i32 timeType, 3: string time, 4: UserFilter userFilter);
	
	/**
	 * 获取基于系统环境的统计指标
	 */
	list<WebStats> getWebStatsForSysEnv(1: i32 webId, 2: i32 timeType, 3: string time, 4: i32 sysType, 5: UserFilter userFilter);
	
	/**
	 * 获取基于小时段的统计指标
	 */
	list<WebStats> getWebStatsForHour(1: i32 webId, 2: i32 timeType, 3: string time, 4: UserFilter userFilter);
}
