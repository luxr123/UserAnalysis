package com.tracker.common.log;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * apache服务器记录的用户访问日志 tjpv.gif
 * @author jason.hua
 *
 */
public class ApachePVLog extends ApacheLog  implements Serializable{
	private static final long serialVersionUID = 3215203015871301411L;

	public static enum FIELDS{
		serverIp, userAgent, serverLogTime, visitStatus, 
		ip, webId, cookieId, cookieCreateTime, userId, userType,
		colorDepth, isCookieEnabled, language, screen,
		refType, refDomain, refKeyword,
		visitTime,  curUrl,title, referrer
	}

	protected static Map<String, Field> FIELD_MAP = new HashMap<String, Field>();
	static{
		try {
			FIELD_MAP.put("ip", ApachePVLog.class.getDeclaredField("ip"));
			FIELD_MAP.put("webId", ApachePVLog.class.getDeclaredField("webId"));
			FIELD_MAP.put("uid", ApachePVLog.class.getDeclaredField("userId"));
			FIELD_MAP.put("utype", ApachePVLog.class.getDeclaredField("userType"));
			FIELD_MAP.put("ckid", ApachePVLog.class.getDeclaredField("cookieId"));
			FIELD_MAP.put("ckct", ApachePVLog.class.getDeclaredField("cookieCreateTime"));
			FIELD_MAP.put("cd", ApachePVLog.class.getDeclaredField("colorDepth"));
			FIELD_MAP.put("ck", ApachePVLog.class.getDeclaredField("isCookieEnabled"));
			FIELD_MAP.put("la", ApachePVLog.class.getDeclaredField("language"));
			FIELD_MAP.put("sc", ApachePVLog.class.getDeclaredField("screen"));
			FIELD_MAP.put("re", ApachePVLog.class.getDeclaredField("referrer"));
			FIELD_MAP.put("u", ApachePVLog.class.getDeclaredField("curUrl"));
			FIELD_MAP.put("tl", ApachePVLog.class.getDeclaredField("title"));
			FIELD_MAP.put("vt", ApachePVLog.class.getDeclaredField("visitTime"));
			FIELD_MAP.put("reftype", ApachePVLog.class.getDeclaredField("refType"));
			FIELD_MAP.put("refd", ApachePVLog.class.getDeclaredField("refDomain"));
			FIELD_MAP.put("refsubd", ApachePVLog.class.getDeclaredField("refSubDomin"));
			FIELD_MAP.put("refkw", ApachePVLog.class.getDeclaredField("refKeyword"));
		} catch (NoSuchFieldException e) {
			logger.error("ApachePVLog Field", e);
		} catch (SecurityException e) {
			logger.error("ApachePVLog Field", e);
		}
	}
	
	//类型名
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
	public Long cookieCreateTime; //cookie创建时间
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
	public Integer refType;//访问来源，直接访问（1），搜索引擎（2），其他外部链接（3） 
	public String refDomain;//访问来源域名
	public String refSubDomin;//搜索引擎子域名，只在refType = 2时有效
	public String refKeyword;//搜索关键词，只在refType = 2时有效
	
	/**
	 * 用户浏览信息
	 */
	public Long visitTime;//用户访问时间，以当地所处时区为准
	public String curUrl;//当前页
	public String title;//当前页面标题
	public String referrer;//上一页Url
	
	public ApachePVLog(String logType){
		this.logType = logType;
	}
	
	public ApachePVLog(){}

	@Override
	public void setData(String ip, long serverLogTime, String userAgent, String visitStatus, String jsData) {
		this.serverIp = ip;
		this.serverLogTime = serverLogTime;
		this.userAgent = userAgent;
		this.visitStatus = visitStatus;
		super.initJsData(jsData, FIELD_MAP);
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
		if(colorDepth == null || isCookieEnabled == null || language == null || screen == null)
			return false;
		
		//来源， 访问页
		if(refType == null || curUrl == null)
			return false;
		return true;
	}

	public String getServerIp() {
		return serverIp;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getUserAgent() {
		return userAgent;
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

	public String getWebId() {
		return webId;
	}

	public void setWebId(String webId) {
		this.webId = webId;
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

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public String getCurUrl() {
		return curUrl;
	}

	public void setCurUrl(String curUrl) {
		this.curUrl = curUrl;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Long getVisitTime() {
		return visitTime;
	}

	public void setVisitTime(Long visitTime) {
		this.visitTime = visitTime;
	}

	public Integer getRefType() {
		return refType;
	}

	public void setRefType(Integer refType) {
		this.refType = refType;
	}

	public String getRefDomain() {
		return refDomain;
	}

	public void setRefDomain(String refDomain) {
		this.refDomain = refDomain;
	}

	public String getRefSubDomin() {
		return refSubDomin;
	}

	public void setRefSubDomin(String refSubDomin) {
		this.refSubDomin = refSubDomin;
	}

	public String getRefKeyword() {
		return refKeyword;
	}

	public void setRefKeyword(String refKeyword) {
		this.refKeyword = refKeyword;
	}
	
	public static List<String> castToList(FIELDS[] fieldsArr){
		List<String> fields = new ArrayList<String>();
		for(Object obj: fieldsArr){
			fields.add(obj.toString());
		}
		return fields;
	}
	
}
