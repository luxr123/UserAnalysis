package com.tracker.db.dao.webstats.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.data.useragent.UserAgentUtil;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.utils.EasyPartion;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.Util;

/**
 * @Author: kris.chen
 * @CreateDate: [2014年8月27日]
 * @Version: [v1.0]
 * 
 */
@HBaseTable(tableName = "log_website_regions", defaultFamily = "infomation")
public class WebVisitLog {
	public static final String ROW_SPLIT = "-"; // char(16）用于分隔符
	@HBaseColumn(qualifier = "visitType")
	public Integer visitType; // 访问类型

	// apache日志字段
	@HBaseColumn(qualifier = "serverIp")
	public String serverIp; // apache获取的ip
	@HBaseColumn(qualifier = "userAgent")
	public String userAgent;// 用户userAgent
	@HBaseColumn(qualifier = "serverLogTime")
	public Long serverLogTime;// 服务器记录日志时间
	@HBaseColumn(qualifier = "visitStatus")
	public String visitStatus;// status

	// js获取的数据
	@HBaseColumn(qualifier = "ip")
	public String ip;// 网站获取的ip
	@HBaseColumn(qualifier = "webId")
	public String webId;// 网站标识
	@HBaseColumn(qualifier = "cookieId")
	public String cookieId;// cookieId
	@HBaseColumn(qualifier = "cookieCreateTime")
	public Long cookieCreateTime; // cookie创建时间（秒级）
	@HBaseColumn(qualifier = "userId")
	public String userId; // 用户id
	@HBaseColumn(qualifier = "userType")
	public Integer userType; // 用户类型，经理人（1），猎头（2), 匿名用户（3）
	@HBaseColumn(qualifier = "colorDepth")
	public String colorDepth;// 屏幕颜色
	@HBaseColumn(qualifier = "isCookieEnabled")
	public Boolean isCookieEnabled;// 是否支持cookie
	@HBaseColumn(qualifier = "language")
	public String language;// 语言环境
	@HBaseColumn(qualifier = "screen")
	public String screen;// 屏幕分辩率
	@HBaseColumn(qualifier = "referrer")
	public String referrer;// 上一页Url
	@HBaseColumn(qualifier = "curUrl")
	public String curUrl;// 当前页
	@HBaseColumn(qualifier = "title")
	public String title;// 当前页面标题
	@HBaseColumn(qualifier = "visitTime")
	public Long visitTime;// 用户访问时间，以当地所处时区为准
	@HBaseColumn(qualifier = "refType")
	public Integer refType;// 访问来源，直接访问（1），搜索引擎（2），其他外部链接（3）
	@HBaseColumn(qualifier = "refDomain")
	public String refDomain;// 访问来源域名
	@HBaseColumn(qualifier = "refSubDomin")
	public String refSubDomin;// 搜索引擎子域名，只在refType = 2时有效
	@HBaseColumn(qualifier = "refKeyword")
	public String refKeyword;// 搜索关键词，只在refType = 2时有效
	@HBaseColumn(qualifier = "os")
	public String os;// 操作系统
	@HBaseColumn(qualifier = "browser")
	public String browser;// 浏览器

	/*
	 * rowkey = cookieId + (Long.Max - time) + userId + userType + visitType +
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
		sb.append(webId).append(ROW_SPLIT);
		sb.append(EasyPartion.getPartition(cookieId,10)).append(ROW_SPLIT);
		sb.append(Long.MAX_VALUE - serverLogTime).append(ROW_SPLIT);
		sb.append(cookieId).append(ROW_SPLIT);
		sb.append(userId == null ? "" : userId).append(ROW_SPLIT);
		sb.append(userType).append(ROW_SPLIT);
		sb.append(visitType).append(ROW_SPLIT);
		sb.append(randValue);
		return sb.toString();
	}

	public Integer getVisitType() {
		return visitType;
	}

	public void setVisitType(Integer visitType) {
		this.visitType = visitType;
	}

	public String getServerIp() {
		return serverIp;
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
		setRefDomain((String) values.get(15));
		setRefKeyword((String) values.get(16));
		setVisitTime(values.get(17) == null ? System.currentTimeMillis() : Long.parseLong(values.get(17).toString()));
		setCurUrl((String) values.get(18));
		setTitle((String) values.get(19));
		setReferrer((String) values.get(20));

	}
	
	public static WebVisitLog getLog(ApachePVLog pvlog){
		WebVisitLog log = new WebVisitLog();
		log.setServerIp(pvlog.getServerIp());
		String userAgent = pvlog.getUserAgent();
		if (userAgent != null) {
			String os = UserAgentUtil.getOSByUserAgent(userAgent);
			String browser = UserAgentUtil.getBrowserByUserAgent(userAgent);
			log.setOs(os);
			log.setBrowser(browser);
			log.setUserAgent(userAgent);
		}
		
		log.setServerLogTime(pvlog.getServerLogTime());
		log.setVisitStatus(pvlog.getVisitStatus());
		log.setIp(pvlog.getIp());
		log.setWebId(pvlog.getWebId());
		log.setCookieId(pvlog.getCookieId());
		
		log.setCookieCreateTime(pvlog.getCookieCreateTime());
		log.setUserId(pvlog.getUserId());
		log.setUserType(pvlog.getUserType());
		log.setColorDepth(pvlog.getColorDepth());
		
		log.setIsCookieEnabled(pvlog.getIsCookieEnabled());
		log.setLanguage(pvlog.getLanguage());
		log.setScreen(pvlog.getScreen());
		
		log.setRefType(pvlog.getRefType());
		log.setRefDomain(pvlog.getRefDomain());
		log.setRefKeyword(pvlog.getRefKeyword());
		
		log.setVisitTime(pvlog.getVisitTime());
		log.setCurUrl(pvlog.getCurUrl());
		log.setTitle(pvlog.getTitle());
		log.setReferrer(pvlog.getReferrer());
		return log;
	}
	
	public static WebVisitLog getLog(Map<String, String> fieldValueMap){
		WebVisitLog log = new WebVisitLog();
		log.setServerIp(fieldValueMap.get(ApachePVLog.FIELDS.serverIp.toString()));
		String userAgent = fieldValueMap.get(ApachePVLog.FIELDS.userAgent.toString());
		if (userAgent != null) {
			String os = UserAgentUtil.getOSByUserAgent(userAgent);
			String browser = UserAgentUtil.getBrowserByUserAgent(userAgent);
			log.setOs(os);
			log.setBrowser(browser);
			log.setUserAgent(userAgent);
		}
		
		if(fieldValueMap.containsKey(ApachePVLog.FIELDS.serverLogTime.toString())){
			log.setServerLogTime(Long.parseLong(fieldValueMap.get(ApachePVLog.FIELDS.serverLogTime.toString())));
		}
		log.setVisitStatus(fieldValueMap.get(ApachePVLog.FIELDS.visitStatus.toString()));
		log.setIp(fieldValueMap.get(ApachePVLog.FIELDS.ip.toString()));
		log.setWebId(fieldValueMap.get(ApachePVLog.FIELDS.webId.toString()));
		log.setCookieId(fieldValueMap.get(ApachePVLog.FIELDS.cookieId.toString()));
		
		if(fieldValueMap.containsKey(ApachePVLog.FIELDS.cookieCreateTime.toString())){
			log.setCookieCreateTime(Long.parseLong(fieldValueMap.get(ApachePVLog.FIELDS.cookieCreateTime.toString())));
		}
		log.setUserId(fieldValueMap.get(ApachePVLog.FIELDS.userId.toString()));
		
		if(fieldValueMap.containsKey(ApachePVLog.FIELDS.userType.toString())){
			log.setUserType(Integer.parseInt(fieldValueMap.get(ApachePVLog.FIELDS.userType.toString())));
		}
		log.setColorDepth(fieldValueMap.get(ApachePVLog.FIELDS.colorDepth.toString()));
		
		if(fieldValueMap.containsKey(ApachePVLog.FIELDS.isCookieEnabled.toString())){
			log.setIsCookieEnabled(Boolean.parseBoolean(fieldValueMap.get(ApachePVLog.FIELDS.isCookieEnabled.toString())));
		}
		log.setLanguage(fieldValueMap.get(ApachePVLog.FIELDS.language.toString()));
		log.setScreen(fieldValueMap.get(ApachePVLog.FIELDS.screen.toString()));
		
		if(fieldValueMap.containsKey(ApachePVLog.FIELDS.refType.toString())){
			log.setRefType(Integer.parseInt(fieldValueMap.get(ApachePVLog.FIELDS.refType.toString())));
		}
		log.setRefDomain(fieldValueMap.get(ApachePVLog.FIELDS.refDomain.toString()));
		log.setRefKeyword(fieldValueMap.get(ApachePVLog.FIELDS.refKeyword.toString()));
		
		if(fieldValueMap.containsKey(ApachePVLog.FIELDS.visitTime.toString())){
			log.setVisitTime(Long.parseLong(fieldValueMap.get(ApachePVLog.FIELDS.visitTime.toString())));
		}
		log.setCurUrl(fieldValueMap.get(ApachePVLog.FIELDS.curUrl.toString()));
		log.setTitle(fieldValueMap.get(ApachePVLog.FIELDS.title.toString()));
		log.setReferrer(fieldValueMap.get(ApachePVLog.FIELDS.referrer.toString()));
		return log;
	}
	
	public static void main(String[] args) {
		Map<String, String> fieldValueMap = new HashMap<String, String>();
		fieldValueMap.put("serverIp", "10.100.2.92");
		WebVisitLog log = WebVisitLog.getLog(fieldValueMap);
	}

}
