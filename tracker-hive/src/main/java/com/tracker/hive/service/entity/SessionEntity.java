package com.tracker.hive.service.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * 会话指标
 * @author jason.hua
 *
 */
public class SessionEntity {
	private int serverDateId;//日期
	private int serverTimeId;//时刻
	private int refType; //来源键
	private String domain; //主域名id
	private String refKeyword;//关键词
	private String pageSign;//会话入口页面
	private int visitorTypeOfDay;//日新老访客类型
	private int visitorTypeOfWeek;//周新老访客类型
	private int visitorTypeOfMonth;//月新老访客类型
	private int visitorTypeOfYear;//年新老访客类型
	
	private long sessionTime;//会话时长（second)
	private long totalPage; //会话中访问的总页数
	private long visitTimes = 1;//访问次数
	private long jumpCount = 0;//跳出次数
	
	private List<PageEntity> pageEntities = new ArrayList<PageEntity>(); //页面实例集合
	private SysEnvEntity sysEnvEntity = new SysEnvEntity(); // 系统环境
	
	
	public SessionEntity(){
		
	}
	
	
	/**
	 * 添加PageEntity实例
	 * @param pageEntity
	 */
	public void addPageEntity(PageEntity pageEntity){
		this.pageEntities.add(pageEntity);
	}
	
	/**
	 * 访问页数加1
	 */
	public void incrementTotalPage(){
		this.totalPage++;
	}
	
	
	public List<PageEntity> getPageEntities() {
		return pageEntities;
	}

	public void setPageEntities(List<PageEntity> pageEntities) {
		this.pageEntities = pageEntities;
	}

	public long getSessionTime() {
		return sessionTime;
	}

	public void setSessionTime(long sessionTime) {
		this.sessionTime = sessionTime;
	}
	
	public void addSessionTime(long time){
		this.sessionTime += time;
	}

	public long getTotalPage() {
		return totalPage;
	}

	public void setTotalPage(long totalPage) {
		this.totalPage = totalPage;
	}
	
	public int getServerDateId() {
		return serverDateId;
	}


	public void setServerDateId(int serverDateId) {
		this.serverDateId = serverDateId;
	}


	public int getServerTimeId() {
		return serverTimeId;
	}


	public void setServerTimeId(int serverTimeId) {
		this.serverTimeId = serverTimeId;
	}

	public int getRefType() {
		return refType;
	}

	public void setRefType(int refType) {
		this.refType = refType;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getRefKeyword() {
		return refKeyword;
	}


	public void setRefKeyword(String refKeyword) {
		this.refKeyword = refKeyword;
	}

	public String getPageSign() {
		return pageSign;
	}

	public void setPageSign(String pageSign) {
		this.pageSign = pageSign;
	}

	public int getVisitorTypeOfDay() {
		return visitorTypeOfDay;
	}


	public void setVisitorTypeOfDay(int visitorTypeOfDay) {
		this.visitorTypeOfDay = visitorTypeOfDay;
	}


	public int getVisitorTypeOfWeek() {
		return visitorTypeOfWeek;
	}


	public void setVisitorTypeOfWeek(int visitorTypeOfWeek) {
		this.visitorTypeOfWeek = visitorTypeOfWeek;
	}


	public int getVisitorTypeOfMonth() {
		return visitorTypeOfMonth;
	}


	public void setVisitorTypeOfMonth(int visitorTypeOfMonth) {
		this.visitorTypeOfMonth = visitorTypeOfMonth;
	}


	public int getVisitorTypeOfYear() {
		return visitorTypeOfYear;
	}


	public void setVisitorTypeOfYear(int visitorTypeOfYear) {
		this.visitorTypeOfYear = visitorTypeOfYear;
	}


	public long getVisitTimes() {
		return visitTimes;
	}


	public void setVisitTimes(long visitTimes) {
		this.visitTimes = visitTimes;
	}


	public long getJumpCount() {
		return jumpCount;
	}


	public void setJumpCount(long jumpCount) {
		this.jumpCount = jumpCount;
	}

	public SysEnvEntity getSysEnvEntity() {
		return sysEnvEntity;
	}

	public void setSysEnvEntity(SysEnvEntity sysEnvEntity) {
		this.sysEnvEntity = sysEnvEntity;
	}
}


