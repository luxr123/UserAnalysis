package com.tracker.data.ip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class WebIp {
	public static void main(String[] args) {
		 Properties prop = System.getProperties();
	     prop.setProperty("http.proxyHost", "10.100.10.100");
	     prop.setProperty("http.proxyPort", "3128");
	        
		String line = getAddressByIp("58.17.23.0");
		System.out.println(line);
		line = getAddressByIp("58.17.23.0");
		System.out.println(line);
		line = getAddressByIp("58.17.23.0");
		System.out.println(line);
	}

	/**
	 * 
	 * @param IP
	 * @return
	 */
	public static String getAddressByIp(String IP) {
		String resout = "";
		try {
			resout = getJsonContent("http://www.ip138.com/ips1388.asp?ip=" +  IP + "&action=2");
		} catch (Exception e) {
			e.printStackTrace();
			resout = "获取IP地址异常：" + e.getMessage();
		}
		return resout;
	}

	public static String getJsonContent(String urlStr) {
		try {// 获取HttpURLConnection连接对象
			URL url = new URL(urlStr);
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			// 设置连接属性
			httpConn.setConnectTimeout(3000);
			httpConn.setDoInput(true);
			httpConn.setRequestMethod("GET");
			// 获取相应码
			int respCode = httpConn.getResponseCode();
			String str="";
			if (respCode == 200) {
				str = ConvertStream2Json(httpConn.getInputStream());
			}
//			httpConn.disconnect();
			return str;
			
		} catch (MalformedURLException e) {
			
		} catch (IOException e) {
			
		}
		return "";
	}

	private static String ConvertStream2Json(InputStream inputStream) {
		String line = null;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "gb2312"));
			while((line = br.readLine()) != null){
				if(line.indexOf("本站主数据：") > 0){
					int startIndex = line.indexOf("本站主数据：");
					int endIndex = line.indexOf("</li><li>参考数据一");
					line = line.substring(startIndex + 6, endIndex);
					break;	
				}
			}
		} catch (IOException e) {
			
		}
		
		return line;
	}
}