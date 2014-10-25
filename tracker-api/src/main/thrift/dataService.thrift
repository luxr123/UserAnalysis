namespace java com.tracker.api.thrift.data
namespace php DataService

/**
 * 实体类
 */ 
struct FilterEntry{
	1: required string name, //显示名
	2: required i32 id 
}

struct SEFilterEntry{
	1: required string name, //显示名
	2: required i32 id, //搜索引擎id
	3: optional i32 searchType //搜索类型
}

/**
 * 网站统计数据服务
 */
service DataService {
	/**
	 * 获取{网站域名->id}对应信息
	 */
	list<FilterEntry> getWebSite();
	
	/**
	 * 获取搜索引擎信息
	 */
	list<FilterEntry> getSearchEngineData();
	
	/**
	 * 获取{seId + searchType -> 站内搜索引擎名 }对应信息， webId为网站id
	 */
	list<SEFilterEntry> getSiteSEAndType(1: i32 webId);
	
	/**
	 * 获取搜索引擎对应的搜索条件
	 */
	list<FilterEntry> getSearchCondition(1:i32 seId, 2:i32 seType);
	
	/**
	 * 获取{用户类型->id}对应信息
	 */
	list<FilterEntry> getUserType(1:i32 webId);
	
	/**
	 * 获取{登录用户类型->id}对应信息
	 */
	list<FilterEntry> getLoginUserType(1:i32 webId);

	/**
	 * 获取{访问类型->id}对应信息
	 */
	list<FilterEntry> getVisitType(1:i32 webId);
	
	/**
	 * 获取搜索类型
	 */
	i32 getVisitTypeOfSearch(1:i32 webId, 2:i32 seId, 3:i32 searchType);
}