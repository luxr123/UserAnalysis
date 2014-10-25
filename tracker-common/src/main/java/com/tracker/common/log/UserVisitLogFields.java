package com.tracker.common.log;

import java.util.ArrayList;
import java.util.List;
/**
 * 
 * 文件名：UserVisitLogFields
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 下午12:06:52
 * 功能描述：hbase表的描述字段,以及获取表信息的结果类
 *
 */
public class UserVisitLogFields {
	public static enum FIELDS{
		webId,country,province,city, visitTime, curUrl, ip, os, browser, isCookieEnabled, language, screen,
		cookieId, isNewVisitor,refType, refDomain,referrer,count,visitStatus,userId,userType,colorDepth,
		cookieCreateTime,urlType,searchLog,refKeyword,visitType,serverLogTime,
		//search fields
		serverIp, userAgent, title, searchParam, searchType, searchConditionJson,
		isCallSE, category, searchShowType, responseTime, totalCount, resultCount, curPageNum
	}
	
	
	public static final String Index_Family = "index";
	public static final String Index_InfoFam = "info";
	public static enum INDEX_FIELDS{
		keyList,count,visitType,visitTime
	}


	private ArrayList<Object> m_fields;
	
	public UserVisitLogFields(){
		m_fields = new ArrayList<Object>(FIELDS.values().length);
		for(int i = 0;i<FIELDS.values().length;i++){
			m_fields.add(null);
		}
	}
	
	public static List<String> castToList(FIELDS[] fieldsArr){
		List<String> fields = new ArrayList<String>();
		for(Object obj: fieldsArr){
			fields.add(obj.toString());
		}
		return fields;
	}
	
	public static List<String> castToList(List<FIELDS> fieldsArr){
		List<String> fields = new ArrayList<String>();
		for(Object obj: fieldsArr){
			fields.add(obj.toString());
		}
		return fields;
	}
	
	public static List<String> getFields(){
		List<String> fields = new ArrayList<String>();
		for(Object obj: UserVisitLogFields.FIELDS.values()){
			fields.add(obj.toString());
		}
		return fields;
	}
	
	public List<Object> getList(){
		return m_fields;
	}
	
	public static List<String> getSpeciedList(FIELDS fields[]){
		List<String> list = new ArrayList<String>();
		for(int i = 0;i< fields.length;i++){
			list.add(fields[i].toString());
		}
		return list;
	}
	
	public void setField(FIELDS field,Object value){
		m_fields.set(field.ordinal(), value);
	}
	
	public Object getRaw(FIELDS field){
		return m_fields.get(field.ordinal());
	}
	
	public String getField(FIELDS field){
		if(String.class.isInstance(m_fields.get(field.ordinal())))
			return (String)m_fields.get(field.ordinal());
		else
			return null;
	}
	
	public Long  getFieldLong(FIELDS field){
		Object obj = m_fields.get(field.ordinal());
		if(obj == null )
			return -1L;
		if(Long.class.isInstance(m_fields.get(field.ordinal())))
			return (Long)m_fields.get(field.ordinal());
		else
			return -1L;
	}
	
//	public String getArea() {
//		return (String)m_fields.get(FIELDS.city.ordinal());
//	}
//	public void setArea(String city) {
//		m_fields.add(FIELDS.city.ordinal(), city);
//	}
//	public String getVisitTime() {
//		return (String)m_fields.get(FIELDS.visitTime.ordinal());
//	}
//	public void setVisitTime(String visitTime) {
//		m_fields.add(FIELDS.visitTime.ordinal(), visitTime);
//	}
//	public String getCurUrl() {
//		return (String)m_fields.get(FIELDS.curUrl.ordinal());
//	}
//	public void setCurUrl(String curUrl) {
//		m_fields.add(FIELDS.curUrl.ordinal(), curUrl);
//	}
//	public String getIp() {
//		return (String)m_fields.get(FIELDS.ip.ordinal());
//	}
//	public void setIp(String ip) {
//		m_fields.add(FIELDS.ip.ordinal(), ip);
//	}
//	public String getos() {
//		return (String)m_fields.get(FIELDS.os.ordinal());
//	}
//	public void setos(String os) {
//		m_fields.add(FIELDS.os.ordinal(),os);
//	}
//	public String getBrowser() {
//		return (String)m_fields.get(FIELDS.browser.ordinal());
//	}
//	public void setBrowser(String browser) {
//		m_fields.add(FIELDS.browser.ordinal(), browser);
//	}
//	public String getIsEnableCookie() {
//		return (String)m_fields.get(FIELDS.isCookieEnable.ordinal());
//	}
//	public void setIsEnableCookie(String isEnableCookie) {
//		m_fields.add(FIELDS.isCookieEnable.ordinal(), isEnableCookie);
//	}
//	
//	public String getLanguage() {
//		return (String)m_fields.get(FIELDS.language.ordinal());
//	}
//	public void setLanguage(String language) {
//		m_fields.add(FIELDS.language.ordinal(), language);
//	}
//	public String getScreen() {
//		return (String)m_fields.get(FIELDS.screen.ordinal());
//	}
//	public void setScreen(String screen) {
//		m_fields.add(FIELDS.screen.ordinal(), screen);
//	}
//	public String getCookieId() {
//		return (String)m_fields.get(FIELDS.cookieId.ordinal());
//	}
//	public void setCookieId(String cookieId) {
//		m_fields.add(FIELDS.cookieId.ordinal(), cookieId);
//	}
//	public String getIsNewVisitor() {
//		return (String)m_fields.get(FIELDS.isNewVisitor.ordinal());
//	}
//	public void setIsNewVisitor(String isNewVisitor) {
//		m_fields.add(FIELDS.isNewVisitor.ordinal(), isNewVisitor);
//	}
}

