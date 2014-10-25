package com.tracker.db.dao.siteSearch.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.data.useragent.UserAgentUtil;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.Util;

/**
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年8月22日 下午4:45:48]
 * @Version: [v1.0]
 * 
 */
@HBaseTable(tableName = "log_website", defaultFamily = "infomation")
public class SearchLog {
	public static final String ROW_SPLIT = "-"; // char(16）用于分隔符

	@HBaseColumn(qualifier = "visitType")
	public Integer visitType; // 访问类型
	/**
	 * apache日志字段
	 */
	@HBaseColumn(qualifier = "serverIp")
	public String serverIp; // apache获取的ip
	@HBaseColumn(qualifier = "userAgent")
	public String userAgent;// 用户userAgent
	@HBaseColumn(qualifier = "serverLogTime")
	public Long serverLogTime;// 服务器记录日志时间
	@HBaseColumn(qualifier = "visitStatus")
	public String visitStatus;// status

	/**
	 * 用户标识
	 */
	@HBaseColumn(qualifier = "ip")
	public String ip;// 网站获取的ip
	@HBaseColumn(qualifier = "cookieId")
	public String cookieId;// cookieId
	@HBaseColumn(qualifier = "cookieCreateTime")
	public Long cookieCreateTime; // cookie创建时间
	@HBaseColumn(qualifier = "userId")
	public String userId; // 用户id
	@HBaseColumn(qualifier = "userType")
	public Integer userType; // 用户类型，经理人（1），猎头（2), 匿名用户（3）

	/**
	 * 客户端系统环境
	 */
	@HBaseColumn(qualifier = "colorDepth")
	public String colorDepth;// 屏幕颜色
	@HBaseColumn(qualifier = "isCookieEnabled")
	public Boolean isCookieEnabled;// 是否支持cookie
	@HBaseColumn(qualifier = "language")
	public String language;// 语言环境
	@HBaseColumn(qualifier = "screen")
	public String screen;// 屏幕分辩率
	@HBaseColumn(qualifier = "os")
	public String os;// 操作系统
	@HBaseColumn(qualifier = "browser")
	public String browser;// 浏览器

	@HBaseColumn(qualifier = "refType")
	public Integer refType;// 访问来源，直接访问（1），搜索引擎（2），其他外部链接（3）

	@HBaseColumn(qualifier = "title")
	public String title;// 当前页面标题
	/**
	 * 搜索信息
	 */
	@HBaseColumn(qualifier = "visitTime")
	public Long visitTime;// 用户搜索时间，以当地所处时区为准
	@HBaseColumn(qualifier = "webId")
	public String webId;// 网站标识
	@HBaseColumn(qualifier = "category")
	public String category; // 站内搜索类型，FoxEngine(猎头搜索经理人）、CaseEngine(搜索case）

	@HBaseColumn(qualifier = "curUrl")
	public String curUrl; // 搜索所在页面url

	@HBaseColumn(qualifier = "searchShowType")
	public Integer searchShowType;// 页面上展示的搜索接口，根据职位名搜索（1）、公司垂直搜索（2）
	@HBaseColumn(qualifier = "searchParam")
	public String searchParam; // 传递给搜索引擎的参数
	@HBaseColumn(qualifier = "searchType")
	public Integer searchType; // 搜索引擎类型， 搜索经理人列表（1）、搜索部门统计（3）
	@HBaseColumn(qualifier = "searchConditionJson")
	public String searchConditionJson; // 搜索条件以及相应的值（json格式）
	@HBaseColumn(qualifier = "isCallSE")
	public Boolean isCallSE; // 是否调用搜索引擎

	/**
	 * 搜索结果
	 */
	@HBaseColumn(qualifier = "responseTime")
	public Integer responseTime;// 搜索响应时间(ms)
	@HBaseColumn(qualifier = "totalCount")
	public Long totalCount; // 搜索结果总数量
	@HBaseColumn(qualifier = "resultCount")
	public Integer resultCount; // 搜索返回数量
	@HBaseColumn(qualifier = "curPageNum")
	public Integer curPageNum; // 当前页数（翻页）

	/*
	 * rowkey = (Long.Max - time) + cookieId + userId + userType + visitType +
	 * webId
	 */
	public String getRowkey() {
		Util.checkNull(visitType);
		Util.checkNull(cookieId);
		Util.checkNull(serverLogTime);
		Util.checkNull(webId);
		Util.checkNull(userType);

		StringBuffer sb = new StringBuffer();
		sb.append(Long.MAX_VALUE - serverLogTime).append(ROW_SPLIT);
		sb.append(cookieId).append(ROW_SPLIT);
		sb.append(userId == null ? "" : userId).append(ROW_SPLIT);
		sb.append(userType).append(ROW_SPLIT);
		sb.append(visitType).append(ROW_SPLIT);
		sb.append(webId);
		return sb.toString();
	}

	public static String generateRowKey(Long serverLogTime, Integer randValue, String cookieId, String userId, Integer userType,
			Integer visitType, String webId) {
		Util.checkNull(cookieId);
		Util.checkNull(serverLogTime);
		Util.checkNull(webId);
		Util.checkNull(visitType);
		Util.checkNull(userType);

		StringBuffer sb = new StringBuffer();
		sb.append(Long.MAX_VALUE - serverLogTime).append(ROW_SPLIT);
		sb.append(cookieId).append(ROW_SPLIT);
		sb.append(userId == null ? "" : userId).append(ROW_SPLIT);
		sb.append(userType).append(ROW_SPLIT);
		sb.append(visitType).append(ROW_SPLIT);
		sb.append(webId).append(ROW_SPLIT);
		sb.append(randValue);
		return sb.toString();
	}

	public String getServerIp() {
		return serverIp;
	}

	public Integer getVisitType() {
		return visitType;
	}

	public void setVisitType(Integer visitType) {
		this.visitType = visitType;
	}

	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public String getSearchConditionJson() {
		return searchConditionJson;
	}

	public void setSearchConditionJson(String searchConditionJson) {
		this.searchConditionJson = searchConditionJson;
	}

	public String getColorDepth() {
		return colorDepth;
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

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	public Integer getRefType() {
		return refType;
	}

	public void setRefType(Integer refType) {
		this.refType = refType;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Boolean getIsCallSE() {
		return isCallSE;
	}

	public void setIsCallSE(Boolean isCallSE) {
		this.isCallSE = isCallSE;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
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

	public String getCookieId() {
		return cookieId;
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

	public Integer getSearchShowType() {
		return searchShowType;
	}

	public void setSearchShowType(Integer searchShowType) {
		this.searchShowType = searchShowType;
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

	public Integer getSearchType() {
		return searchType;
	}

	public void setSearchType(Integer searchType) {
		this.searchType = searchType;
	}

	public void injectValues(List<Object> values) {
		setServerIp((String) values.get(0));
		String userAgent = (String) values.get(1);
		if (userAgent != null) {
			String os = UserAgentUtil.getOSByUserAgent(userAgent);
			String browser = UserAgentUtil.getBrowserByUserAgent(userAgent);
			setOs(os);
			setBrowser(browser);
			setUserAgent(userAgent);
		}
		setServerLogTime(values.get(2) == null ? System.currentTimeMillis() : Long.parseLong(values.get(2).toString()));
		setVisitStatus((String) values.get(3));
		setIp((String) values.get(4));
		setWebId((String) values.get(5));
		setCookieId((String) values.get(6));
		setCookieCreateTime(values.get(7) == null ? System.currentTimeMillis() / 1000 : Long.parseLong(values.get(7).toString()));
		setUserId((String) values.get(8));
		setUserType(values.get(9) == null ? 3 : Integer.parseInt(values.get(9).toString()));
		setColorDepth((String) values.get(10));
		setIsCookieEnabled(values.get(11) == null ? false : Boolean.parseBoolean(values.get(11).toString()));
		setLanguage((String) values.get(12));
		setScreen((String) values.get(13));
		setRefType(values.get(14) == null ? ReferrerType.DIRECT.getValue() : Integer.parseInt(values.get(14).toString()));
		setVisitTime(values.get(15) == null ? System.currentTimeMillis() : Long.parseLong(values.get(15).toString()));
		setCurUrl((String) values.get(16));
		setTitle((String) values.get(17));
		setSearchParam((String) values.get(18));
		setSearchType(values.get(19) == null ? null : Integer.parseInt(values.get(19).toString())); //CaseEngine的searchType为null
		setSearchConditionJson((String) values.get(20));
		setIsCallSE(values.get(21) == null ? false : Boolean.parseBoolean(values.get(21).toString()));
		setCategory((String) values.get(22));
		setSearchShowType(values.get(23) == null ? 1 : Integer.parseInt(values.get(23).toString()));
		setResponseTime(values.get(24) == null ? 0 : Integer.parseInt(values.get(24).toString()));
		setTotalCount(values.get(25) == null ? 0 : Long.parseLong(values.get(25).toString()));
		setResultCount(values.get(26) == null ? 0 : Integer.parseInt(values.get(26).toString()));
		setCurPageNum(values.get(27) == null ? 0 : Integer.parseInt(values.get(27).toString()));
	}
	
	public static SearchLog getLog(ApacheSearchLog apacheSearchLog){
		SearchLog log = new SearchLog();
		log.setServerIp(apacheSearchLog.getServerIp());
		String userAgent = apacheSearchLog.getUserAgent();
		if (userAgent != null) {
			String os = UserAgentUtil.getOSByUserAgent(userAgent);
			String browser = UserAgentUtil.getBrowserByUserAgent(userAgent);
			log.setOs(os);
			log.setBrowser(browser);
			log.setUserAgent(userAgent);
		}
		
		log.setServerLogTime(apacheSearchLog.getServerLogTime());
		log.setVisitStatus(apacheSearchLog.getVisitStatus());
		log.setIp(apacheSearchLog.getIp());
		log.setWebId(apacheSearchLog.getWebId());
		log.setCookieId(apacheSearchLog.getCookieId());
		
		log.setCookieCreateTime(apacheSearchLog.getCookieCreateTime());
		log.setUserId(apacheSearchLog.getUserId());
		log.setUserType(apacheSearchLog.getUserType());
		log.setColorDepth(apacheSearchLog.getColorDepth());
		
		log.setIsCookieEnabled(apacheSearchLog.getIsCookieEnabled());
		log.setLanguage(apacheSearchLog.getLanguage());
		log.setScreen(apacheSearchLog.getScreen());
		
		log.setRefType(apacheSearchLog.getRefType());
		
		log.setVisitTime(apacheSearchLog.getVisitTime());
		log.setCurUrl(apacheSearchLog.getCurUrl());
		log.setTitle(apacheSearchLog.getTitle());
		log.setSearchParam(apacheSearchLog.getSearchParam());
		log.setSearchType(apacheSearchLog.getSearchType());
		log.setSearchConditionJson(apacheSearchLog.getSearchConditionJson());
		log.setIsCallSE(apacheSearchLog.getIsCallSE());
		log.setCategory(apacheSearchLog.getCategory());
		log.setSearchShowType(apacheSearchLog.getSearchShowType());
		log.setResponseTime(apacheSearchLog.getResponseTime());
		log.setTotalCount(apacheSearchLog.getTotalCount());
		log.setResultCount(apacheSearchLog.getResultCount());
		log.setCurPageNum(apacheSearchLog.getCurPageNum());
		return log;
	}

	public static SearchLog getLog(Map<String, String> fieldValueMap){
		SearchLog log = new SearchLog();
		log.setServerIp(fieldValueMap.get(ApacheSearchLog.FIELDS.serverIp.toString()));
		String userAgent = fieldValueMap.get(ApacheSearchLog.FIELDS.userAgent.toString());
		if (userAgent != null) {
			String os = UserAgentUtil.getOSByUserAgent(userAgent);
			String browser = UserAgentUtil.getBrowserByUserAgent(userAgent);
			log.setOs(os);
			log.setBrowser(browser);
			log.setUserAgent(userAgent);
		}
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.serverLogTime.toString())){
			log.setServerLogTime(Long.parseLong(fieldValueMap.get(ApacheSearchLog.FIELDS.serverLogTime.toString())));
		}
		log.setVisitStatus(fieldValueMap.get(ApacheSearchLog.FIELDS.visitStatus.toString()));
		log.setIp(fieldValueMap.get(ApacheSearchLog.FIELDS.ip.toString()));
		log.setWebId(fieldValueMap.get(ApacheSearchLog.FIELDS.webId.toString()));
		log.setCookieId(fieldValueMap.get(ApacheSearchLog.FIELDS.cookieId.toString()));
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.cookieCreateTime.toString())){
			log.setCookieCreateTime(Long.parseLong(fieldValueMap.get(ApacheSearchLog.FIELDS.cookieCreateTime.toString())));
		}
		log.setUserId(fieldValueMap.get(ApacheSearchLog.FIELDS.userId.toString()));
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.userType.toString())){
			log.setUserType(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.userType.toString())));
		}
		log.setColorDepth(fieldValueMap.get(ApacheSearchLog.FIELDS.colorDepth.toString()));
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.isCookieEnabled.toString())){
			log.setIsCookieEnabled(Boolean.parseBoolean(fieldValueMap.get(ApacheSearchLog.FIELDS.isCookieEnabled.toString())));
		}
		log.setLanguage(fieldValueMap.get(ApacheSearchLog.FIELDS.language.toString()));
		log.setScreen(fieldValueMap.get(ApacheSearchLog.FIELDS.screen.toString()));
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.refType.toString())){
			log.setRefType(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.refType.toString())));
		}
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.visitTime.toString())){
			log.setVisitTime(Long.parseLong(fieldValueMap.get(ApacheSearchLog.FIELDS.visitTime.toString())));
		}
		log.setCurUrl(fieldValueMap.get(ApacheSearchLog.FIELDS.curUrl.toString()));
		log.setTitle(fieldValueMap.get(ApacheSearchLog.FIELDS.title.toString()));
		
		log.setSearchParam(fieldValueMap.get(ApacheSearchLog.FIELDS.searchParam.toString()));
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.searchType.toString())){
			log.setSearchType(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.searchType.toString())));
		}
		log.setSearchConditionJson((String) fieldValueMap.get(ApacheSearchLog.FIELDS.searchConditionJson.toString()));
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.isCallSE.toString())){
			log.setIsCallSE(Boolean.parseBoolean(fieldValueMap.get(ApacheSearchLog.FIELDS.isCallSE.toString())));
		}
		log.setCategory((String) fieldValueMap.get(ApacheSearchLog.FIELDS.category.toString()));
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.searchShowType.toString())){
			log.setSearchShowType(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.searchShowType.toString())));
		}
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.responseTime.toString())){
			log.setResponseTime(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.responseTime.toString())));
		}
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.totalCount.toString())){
			log.setTotalCount(Long.valueOf(fieldValueMap.get(ApacheSearchLog.FIELDS.totalCount.toString()).toString()));
		}
		
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.resultCount.toString())){
			log.setResultCount(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.resultCount.toString())));
		}
		if(fieldValueMap.containsKey(ApacheSearchLog.FIELDS.curPageNum.toString())){
			log.setCurPageNum(Integer.parseInt(fieldValueMap.get(ApacheSearchLog.FIELDS.curPageNum.toString())));
		}
		return log;
	}
	
	public static void main(String[] args) {
		SearchLog log = SearchLog.getLog(new HashMap<String, String>());
	}
	
}
