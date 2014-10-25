package com.tracker.common.log;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.FieldHandler;


public abstract class ApacheLog {
	protected static Logger logger = LoggerFactory.getLogger(ApacheLog.class);
	public final static String logTypeField = "logType";
	public final static String SPLIT = "&";
	public final static String PARAM_SPLIT = "=";
	
	public final static String DECODE = "gbk";
	//pv log
	public final static String PV_SIGN = "tjpv";
	
	//search log
	public final static String SEARCH_SIGN = "tjsearch";
	public final static String CATEGORY_SIGN = "cat=FoxEngine";
	
	public final static String APACHE_PV_LOG_TYPE = "apachePVLog";
	public final static String APACHE_SEARCH_LOG_TYPE = "apacheSearchLog";

	
	public static  ApacheLog getApacheLog(String paramData){
		if(paramData.indexOf(PV_SIGN) > -1){
			return new ApachePVLog(APACHE_PV_LOG_TYPE);
		} else if(paramData.indexOf(SEARCH_SIGN) > -1){
			return new ApacheSearchLog(APACHE_SEARCH_LOG_TYPE);
		}
		return null;
	}
	
	protected void initJsData(String jsData, Map<String, Field> fieldMap){
		String[] datas = jsData.split(SPLIT);
		if(datas == null)
			return;

		Map<String, String> dataValueMap = getDataValueMap(datas);
		if(dataValueMap.size() == 0)
			return;
		for(String fieldStr: fieldMap.keySet()){
			String dataValue = dataValueMap.get(fieldStr);
			if(dataValue == null)
				continue;
			Field field = fieldMap.get(fieldStr);
			try {
				field.set(this, FieldHandler.stringToObject(field.getType(), dataValue));
			} catch (IllegalArgumentException e) {
				logger.error("setJsData", e);
			} catch (IllegalAccessException e) {
				logger.error("setJsData", e);
			} 
		}
		
		if(this instanceof ApacheSearchLog){
			ApacheSearchLog log = (ApacheSearchLog)this;
			log.setSearchConditionJson(log.getCategory(), dataValueMap);
		}
	}
	
	/**
	 * 获取对应的值
	 * @param datas
	 * @return
	 */
	private Map<String, String> getDataValueMap(String[] datas){
		Map<String, String> dataValueMap = new HashMap<String, String>();
		for(String data: datas){
			String[] params = data.split(PARAM_SPLIT);
			if(params == null || params.length < 2)
				continue;
			if(params[1].length() == 0)
				continue;
			try {
				params[1] = URLDecoder.decode(params[1], "UTF-8");
			} catch (UnsupportedEncodingException e) {
				logger.error("URLDecoder error: " + params[1], e);
			}
			if(params[1].equalsIgnoreCase("undefined"))
				continue;
			dataValueMap.put(params[0], params[1]);
		}
		return dataValueMap;
	}
	
	public abstract void setData(String ip, long serverLogTime, String userAgent, String visitStatus, String paramData);
	
	//TODO clean log
	public abstract boolean cleanLog();
	
	public abstract String getLogType();

}
