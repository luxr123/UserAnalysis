package com.tracker.common.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.hdfs.HdfsProxy;
import com.tracker.common.utils.Config.RichProperties;

public class ConfigExt {
	private static Logger LOG = LoggerFactory.getLogger(ConfigExt.class);
	
	public static RichProperties getProperties(String distributeLoc,String filename) {
		RichProperties richProp = new RichProperties();
		HdfsProxy.setFSLocation(distributeLoc);
		HdfsProxy hdfsProxy = new HdfsProxy();
		byte[] bytearray = hdfsProxy.readFileFromHdfs(filename);
		ByteArrayInputStream bais = new ByteArrayInputStream(bytearray);
		try {
			Properties prop = new Properties();
			if (bais != null) prop.load(bais);
			richProp.putAll(prop);
			return richProp;
		} catch(Exception e) { 
			LOG.error("Error while loading file: " + filename, e);
		} finally {
			try {
				if (bais != null) bais.close();
			} catch (IOException e) { }
		}
		return richProp;
	}
	
	public static void main(String[] args) {
		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
		String configFile = java.lang.System.getenv("COMMON_CONFIG");
		Properties properties = ConfigExt.getProperties(hdfsLocation, configFile);
		System.out.println(hdfsLocation + ":" + configFile);
		System.out.println(properties.getProperty("hbase.zookeeper.quorum"));
	}
}
