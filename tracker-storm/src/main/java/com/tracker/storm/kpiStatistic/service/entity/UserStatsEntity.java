package com.tracker.storm.kpiStatistic.service.entity;

import java.io.Serializable;

public class UserStatsEntity implements Serializable{
	private static final long serialVersionUID = 4488238308518931899L;
	private String rowKeyOfCookie;
	private String rowKeyOfUser;
	private long stayTime;
	public String getRowKeyOfCookie() {
		return rowKeyOfCookie;
	}
	public void setRowKeyOfCookie(String rowKeyOfCookie) {
		this.rowKeyOfCookie = rowKeyOfCookie;
	}
	public String getRowKeyOfUser() {
		return rowKeyOfUser;
	}
	public void setRowKeyOfUser(String rowKeyOfUser) {
		this.rowKeyOfUser = rowKeyOfUser;
	}
	public long getStayTime() {
		return stayTime;
	}
	public void setStayTime(long stayTime) {
		this.stayTime = stayTime;
	}
	
	public static enum statsType{
		cookieIdType, userIdType
	}
}
