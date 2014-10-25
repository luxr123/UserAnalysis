package com.tracker.common.log;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.log.condition.CaseSearchCondition;
import com.tracker.common.log.condition.ManagerSearchCondition;
import com.tracker.common.utils.FieldHandler;
import com.tracker.common.utils.JsonUtil;

/**
 * 网站搜索日志  
 * @author jason.hua
 *
 */
public class ApacheSearchLog extends ApacheLog implements Serializable{
	private static final long serialVersionUID = -1186703643923004097L;
	
	public static enum FIELDS{
		serverIp, userAgent, serverLogTime, visitStatus, 
		ip, webId, cookieId, cookieCreateTime, userId, userType,
		colorDepth, isCookieEnabled, language, screen,
		refType, visitTime, curUrl,title, searchParam, searchType, searchConditionJson,
		isCallSE, category, searchShowType, responseTime, totalCount, resultCount, curPageNum
	}
	
	public static List<Field> FIELD_LIST = new ArrayList<Field>();
	static {
		try {
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.serverIp.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.userAgent.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.serverLogTime.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.visitStatus.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.ip.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.cookieId.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.cookieCreateTime.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.userId.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.userType.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.webId.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.visitTime.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.curUrl.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.category.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.searchShowType.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.responseTime.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.totalCount.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.resultCount.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.curPageNum.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.searchParam.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.searchType.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.searchConditionJson.toString()));
			FIELD_LIST.add(ApacheSearchLog.class.getDeclaredField(FIELDS.isCallSE.toString()));
		} catch (NoSuchFieldException e) {
			logger.error("ManagerSearchCondition Field", e);
		} catch (SecurityException e) {
			logger.error("ManagerSearchCondition Field", e);
		}
	}
	
	public static Map<String, Field> FIELD_MAP = new HashMap<String, Field>();
	static{
		try {
			FIELD_MAP.put("ip", ApacheSearchLog.class.getDeclaredField(FIELDS.ip.toString()));
			FIELD_MAP.put("webId", ApacheSearchLog.class.getDeclaredField(FIELDS.webId.toString()));
			FIELD_MAP.put("uid", ApacheSearchLog.class.getDeclaredField(FIELDS.userId.toString()));
			FIELD_MAP.put("utype", ApacheSearchLog.class.getDeclaredField(FIELDS.userType.toString()));
			FIELD_MAP.put("ckid", ApacheSearchLog.class.getDeclaredField(FIELDS.cookieId.toString()));
			FIELD_MAP.put("ckct", ApacheSearchLog.class.getDeclaredField(FIELDS.cookieCreateTime.toString()));
			
			FIELD_MAP.put("cd", ApacheSearchLog.class.getDeclaredField(FIELDS.colorDepth.toString()));
			FIELD_MAP.put("ck", ApacheSearchLog.class.getDeclaredField(FIELDS.isCookieEnabled.toString()));
			FIELD_MAP.put("la", ApacheSearchLog.class.getDeclaredField(FIELDS.language.toString()));
			FIELD_MAP.put("sc", ApacheSearchLog.class.getDeclaredField(FIELDS.screen.toString()));
			
			FIELD_MAP.put("u", ApacheSearchLog.class.getDeclaredField(FIELDS.curUrl.toString()));
			FIELD_MAP.put("tl", ApacheSearchLog.class.getDeclaredField(FIELDS.title.toString()));
			FIELD_MAP.put("vt", ApacheSearchLog.class.getDeclaredField(FIELDS.visitTime.toString()));
			
			FIELD_MAP.put("cat", ApacheSearchLog.class.getDeclaredField(FIELDS.category.toString()));
			FIELD_MAP.put("curPage", ApacheSearchLog.class.getDeclaredField(FIELDS.curPageNum.toString()));
			FIELD_MAP.put("type", ApacheSearchLog.class.getDeclaredField(FIELDS.searchShowType.toString()));
			FIELD_MAP.put("responseTime", ApacheSearchLog.class.getDeclaredField(FIELDS.responseTime.toString()));
			FIELD_MAP.put("totalCount", ApacheSearchLog.class.getDeclaredField(FIELDS.totalCount.toString()));
			FIELD_MAP.put("resultCount", ApacheSearchLog.class.getDeclaredField(FIELDS.resultCount.toString()));
			FIELD_MAP.put("searchParam", ApacheSearchLog.class.getDeclaredField(FIELDS.searchParam.toString()));
			FIELD_MAP.put("searchType", ApacheSearchLog.class.getDeclaredField(FIELDS.searchType.toString()));
			FIELD_MAP.put("isCallSE", ApacheSearchLog.class.getDeclaredField(FIELDS.isCallSE.toString()));
		} catch (NoSuchFieldException e) {
			logger.error("ApacheSearchLog Field", e);
		} catch (SecurityException e) {
			logger.error("ApacheSearchLog Field", e);
		}
	}
	
	//kafka中topic
	public String logType;
	
	/**
	 * apache日志字段
	 */
	public String serverIp; //apache获取的ip
	public String userAgent;//用户userAgent
	public Long serverLogTime;//服务器记录日志时间（毫秒级）
	public String visitStatus;// status
	
	/**
	 * 用户标识
	 */
	public String ip;//网站获取的ip
	public String webId;//网站标识
	public String cookieId;//cookieId
	public Long cookieCreateTime; //cookie创建时间（秒级）
	public String userId; //用户id 
	public Integer userType; //用户类型，经理人（1），猎头（2), 匿名用户（3）
	
	/**
	 * 客户端系统环境
	 */
	public String colorDepth;//屏幕颜色
	public Boolean isCookieEnabled;//是否支持cookie
	public String language;//语言环境
	public String screen;//屏幕分辩率
	
	/**
	 * 访问来源
	 */
	public Integer refType = ReferrerType.DIRECT.getValue();//访问来源，直接访问（1）
	
	/**
	 * 搜索信息
	 */
	public String title;//当前页面标题
	public String curUrl; //搜索所在页面url
	public Long visitTime;//用户搜索时间，以当地所处时区为准
	public String category; //站内搜索类型，FoxEngine(猎头搜索经理人）、CaseEngine(搜索case）
	public Integer searchShowType;//页面上展示的搜索接口，根据职位名搜索（1）、公司垂直搜索（2）
	public String searchParam; //传递给搜索引擎的参数
	public Integer searchType; // 搜索引擎类型， 搜索经理人列表（1）、搜索部门统计（3）
	public String searchConditionJson; //搜索条件以及相应的值（json格式）
	public Boolean isCallSE; //是否调用搜索引擎
	
	/**
	 * 搜索结果
	 */
	public Integer responseTime = 0;//搜索响应时间(ms)
	public Long totalCount = 0L; //搜索结果总数量
	public Integer resultCount = 0; //搜索返回数量
	public Integer curPageNum = 1; //当前页数（翻页）
	
	public ApacheSearchLog(){
	}
	
	public ApacheSearchLog(String logType){
		this.logType = logType;
	}
	
	@Override
	public void setData(String ip, long serverLogTime, String userAgent, String visitStatus, String jsData) {
		this.serverIp = ip;
		this.serverLogTime = serverLogTime;
		this.userAgent = userAgent;
		this.visitStatus = visitStatus;
		super.initJsData(jsData, FIELD_MAP);
		if(totalCount == 0){
			totalCount = resultCount.longValue();
		}
		if(cookieCreateTime != null)
			cookieCreateTime = cookieCreateTime * 1000;
	}
	
	@Override
	public String getLogType() {
		return logType;
	}

	@Override
	public boolean cleanLog() {
		//apache log
		if(userAgent == null && serverLogTime == null)
			return false;
		
		//用户标识
		if(ip == null || webId == null || cookieId == null || userType == null)
			return false;
		if(cookieCreateTime == null){
			cookieCreateTime = System.currentTimeMillis();
		} else {
			cookieCreateTime = cookieCreateTime * 1000;
		}
		
		//客户端系统环境
		if(colorDepth == null || isCookieEnabled == null || language == null || screen == null )
			return false;
		
		//搜索信息
		if(category == null || curUrl == null || searchShowType == null || searchConditionJson == null || isCallSE == null)
			return false;
		return true;
	}
	
	/**
	 * 设置搜索条件json值
	 */
	public void setSearchConditionJson(String category, Map<String, String> dataValueMap){
		if(category.equalsIgnoreCase(ManagerSearchCondition.SEARCH_ENGIN_NAME)){
			ManagerSearchCondition searchCondition = new ManagerSearchCondition();
			for(String fieldStr: ManagerSearchCondition.FIELD_MAP.keySet()){
				String dataValue = dataValueMap.get(fieldStr);
				if(dataValue == null)
					continue;
				Field field = ManagerSearchCondition.FIELD_MAP.get(fieldStr);
				try {
					field.set(searchCondition, FieldHandler.stringToObject(field.getType(), dataValue));
				} catch (IllegalArgumentException e) {
					logger.error("setJsData", e);
				} catch (IllegalAccessException e) {
					logger.error("setJsData", e);
				} 
			}
			this.searchConditionJson = JsonUtil.toJson(searchCondition);
		} else if(category.equalsIgnoreCase(CaseSearchCondition.SEARCH_ENGIN_NAME)){
			CaseSearchCondition searchCondition = new CaseSearchCondition();
			for(String fieldStr: CaseSearchCondition.FIELD_MAP.keySet()){
				String dataValue = dataValueMap.get(fieldStr);
				if(dataValue == null)
					continue;
				Field field = CaseSearchCondition.FIELD_MAP.get(fieldStr);
				try {
					field.set(searchCondition, FieldHandler.stringToObject(field.getType(), dataValue));
				} catch (IllegalArgumentException e) {
					logger.error("setJsData", e);
				} catch (IllegalAccessException e) {
					logger.error("setJsData", e);
				} 
			}
			this.searchConditionJson = JsonUtil.toJson(searchCondition);
		} 
	}
	
	public String getServerIp() {
		return serverIp;
	}

	public Boolean getIsCallSE() {
		return isCallSE;
	}

	public void setIsCallSE(Boolean isCallSE) {
		this.isCallSE = isCallSE;
	}

	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public String getColorDepth() {
		return colorDepth;
	}

	public Integer getRefType() {
		return refType;
	}

	public void setRefType(Integer refType) {
		this.refType = refType;
	}

	public void setColorDepth(String colorDepth) {
		this.colorDepth = colorDepth;
	}

	public Boolean getIsCookieEnabled() {
		return isCookieEnabled;
	}

	public void setIsCookieEnabled(Boolean isCookieEnabled) {
		this.isCookieEnabled = isCookieEnabled;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getScreen() {
		return screen;
	}

	public void setScreen(String screen) {
		this.screen = screen;
	}

	public Integer getSearchType() {
		return searchType;
	}

	public String getSearchConditionJson() {
		return searchConditionJson;
	}

	public void setSearchConditionJson(String searchConditionJson) {
		this.searchConditionJson = searchConditionJson;
	}

	public void setSearchType(Integer searchType) {
		this.searchType = searchType;
	}

	public Integer getSearchShowType() {
		return searchShowType;
	}

	public void setSearchShowType(Integer searchShowType) {
		this.searchShowType = searchShowType;
	}

	public Long getServerLogTime() {
		return serverLogTime;
	}

	public void setServerLogTime(Long serverLogTime) {
		this.serverLogTime = serverLogTime;
	}

	public String getVisitStatus() {
		return visitStatus;
	}

	public void setVisitStatus(String visitStatus) {
		this.visitStatus = visitStatus;
	}

	public Long getVisitTime() {
		return visitTime;
	}

	public void setVisitTime(Long visitTime) {
		this.visitTime = visitTime;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getWebId() {
		return webId;
	}

	public void setWebId(String webId) {
		this.webId = webId;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getCurUrl() {
		return curUrl;
	}

	public void setCurUrl(String curUrl) {
		this.curUrl = curUrl;
	}

	public String getCookieId() {
		return cookieId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public void setCookieId(String cookieId) {
		this.cookieId = cookieId;
	}

	public Long getCookieCreateTime() {
		return cookieCreateTime;
	}

	public void setCookieCreateTime(Long cookieCreateTime) {
		this.cookieCreateTime = cookieCreateTime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Integer getUserType() {
		return userType;
	}

	public void setUserType(Integer userType) {
		this.userType = userType;
	}

	public Integer getResponseTime() {
		return responseTime;
	}

	public void setResponseTime(Integer responseTime) {
		this.responseTime = responseTime;
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}

	public Integer getResultCount() {
		return resultCount;
	}

	public void setResultCount(Integer resultCount) {
		this.resultCount = resultCount;
	}

	public Integer getCurPageNum() {
		return curPageNum;
	}

	public void setCurPageNum(Integer curPageNum) {
		this.curPageNum = curPageNum;
	}

	public String getSearchParam() {
		return searchParam;
	}

	public void setSearchParam(String searchParam) {
		this.searchParam = searchParam;
	}
	
	public String toString(){
		return JsonUtil.toJson(this);
	}
	
	public static List<String> castToList(FIELDS[] fieldsArr){
		List<String> fields = new ArrayList<String>();
		for(Object obj: fieldsArr){
			fields.add(obj.toString());
		}
		return fields;
	}
	
	public static void main(String[] args) {
		String json = "{'webId':'1','category':'FoxEngine', 'searchType':'1'}";
		ApacheSearchLog log = JsonUtil.toObject(json, ApacheSearchLog.class);
		System.out.println(log.webId);
		System.out.println(log.category);
	}
}