package com.tracker.storm.kpiStatistic.service.entity;

import java.io.Serializable;

public class WebSiteKpiDimension implements Serializable{
	private static final long serialVersionUID = 1947421999410164308L;
	private String webId;
	private String date;
	private Integer hour;
	private Integer userType;
	private Integer visitorType;
	
	private String pageSign;
	
	private Integer countryId;
	private Integer provinceId;
	private Integer cityId;
	
	private Integer refType;
	private String refDomain;
	private String refKeyword;
	
	private String colorDepth;
	private Boolean isCookieEnabled;
	private String language;
	private String screen;
	private String os;
	private String browser;

	public String getPageSign() {
		return pageSign;
	}


	public void setPageSign(String pageSign) {
		this.pageSign = pageSign;
	}


	public String getWebId() {
		return webId;
	}


	public void setWebId(String webId) {
		this.webId = webId;
	}


	public String getDate() {
		return date;
	}


	public void setDate(String date) {
		this.date = date;
	}


	public Integer getHour() {
		return hour;
	}


	public void setHour(Integer hour) {
		this.hour = hour;
	}


	public Integer getVisitorType() {
		return visitorType;
	}
	public Integer getUserType() {
		return userType;
	}
	public void setUserType(Integer userType) {
		this.userType = userType;
	}
	public void setVisitorType(Integer visitorType) {
		this.visitorType = visitorType;
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
		if(language != null){
			language = language.toLowerCase();
		}
		this.language = language;
	}
	
	public Integer getCountryId() {
		return countryId;
	}
	public void setCountryId(Integer countryId) {
		this.countryId = countryId;
	}
	public Integer getProvinceId() {
		return provinceId;
	}
	public void setProvinceId(Integer provinceId) {
		this.provinceId = provinceId;
	}
	public Integer getCityId() {
		return cityId;
	}
	public void setCityId(Integer cityId) {
		this.cityId = cityId;
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
	public String getRefDomain() {
		return refDomain;
	}
	public void setRefDomain(String refDomain) {
		this.refDomain = refDomain;
	}
	public String getRefKeyword() {
		return refKeyword;
	}
	public void setRefKeyword(String refKeyword) {
		this.refKeyword = refKeyword;
	}
}
