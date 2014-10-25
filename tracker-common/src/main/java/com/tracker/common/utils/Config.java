package com.tracker.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static Logger logger = LoggerFactory.getLogger(Config.class);
	
	public static  RichProperties getConfig(String filename){
		RichProperties prop = new RichProperties();
		prop.putAll(getProperties(filename));
		return prop;
	}
	
	private static Properties getProperties(String filename) {
		Properties prop = new Properties();
		InputStream is = null;
		try {
			is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
			if (is != null) prop.load(is);
			return prop;
		} catch(Exception e) { 
			logger.error("Error while loading file: " + filename, e);
			return prop;
		} finally {
			try {
				if (is != null) is.close();
			} catch (IOException e) { }
		}
	}
	
	public static class RichProperties extends Properties{
		private static final long serialVersionUID = 1L;

		/**
		 * 获取Int值
		 * @param name
		 * @return
		 */
		public Integer getInt(String name){
			return getInt(name, null);
		}
		
		public Integer getInt(String name, Integer defaultValue){
			String value = super.getProperty(name);
			if(value == null){
				return defaultValue;
			} else {
				return Integer.parseInt(value.trim());
			}
		}
		
		public Double getDouble(String name){
			return getDouble(name, null);
		}
		
		public Double getDouble(String name, Double defaultValue){
			String value = super.getProperty(name);
			if(value == null){
				return defaultValue;
			} else {
				return Double.parseDouble(value.trim());
			}
		}
		
		/**
		 * 获取String值
		 * @param name
		 * @return
		 */
		public String getString(String name){
			return getString(name, null);
		}
		
		public String getString(String name, String defaultValue){
			String value = super.getProperty(name);
			if(value == null){
				return defaultValue;
			} else {
				return value.trim();
			}
		}
		
		/**
		 * 获取集合，各个属性用“，”分割
		 * @param name
		 * @return
		 */
		public List<String> getList(String name){
			List<String> list = new ArrayList<String>();
			String value = super.getProperty(name);
			if(value != null) {
				for(String str : value.split(",")) {
					list.add(str.trim());
				}
			}
			return list;
		}
		
		public boolean getBoolean(String name, boolean defaultValue){
			String value = super.getProperty(name);
			if(value != null){
				return Boolean.parseBoolean(value);
			}
			return defaultValue;
		}
	}
}
