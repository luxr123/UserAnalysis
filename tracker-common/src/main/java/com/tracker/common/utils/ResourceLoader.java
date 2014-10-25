package com.tracker.common.utils;

import java.io.InputStream;

public class ResourceLoader {
	public static String getFilePath(String filename){
		String filePath = Thread.currentThread().getContextClassLoader().getResource(filename).getPath();
		return filePath;
	}
	
	public static InputStream getFileInputStream(String filename){
		return Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
	}
}
