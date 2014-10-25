package com.tracker.data.ip;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.tracker.common.data.ip.LocationConstants;
import com.tracker.common.data.ip.LocationParser;
import com.tracker.common.utils.ResourceLoader;


/**
 * 根据纯真ip数据库：qqwry(转为ip.txt）和网上查询，生成ipArea.txt和universityLocation.txt
 * ipArea.txt：生成字段，startIp、endIp、中国/国外、省份/直辖市/特区、城市名
 * universityLocation.txt：用于转化大学为具体的国家、省、市
 * @author jason.hua
 *
 */
public class IpLocationInit {
	private Map<String, String> universityLocation = new HashMap<String, String>();

	private static BufferedWriter bw = null;

	public static void main(String[] args) throws IOException {
		 Properties prop = System.getProperties();
	     prop.setProperty("http.proxyHost", "10.100.10.100");
	     prop.setProperty("http.proxyPort", "3128");

	     IpLocationInit locationInit = new IpLocationInit();
	     locationInit.init();
	}
	
	public void init() throws IOException {
	    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/ip/ipArea.txt"), "utf-8"));
		BufferedReader br = new BufferedReader(new InputStreamReader(ResourceLoader.getFileInputStream("source/ip.txt"), "gb2312"));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] strs = parseLine(line);
			if (strs == null)
				continue;
			String location = getLocation(strs[0], strs[2]);
			bw.write(strs[0] + LocationConstants.SPLIT + strs[1] + LocationConstants.SPLIT + location + "\n");
		}
		bw.close();
		
//		String json = JsonUtil.toJson(universityLocation);
//		System.out.println(json);
		writeToFile(universityLocation, "data/ip/universityLocation.txt");
	}
	
	public void writeToFile(Map<String, String> universityLocation, String fileName) throws IOException{
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
		Iterator<Entry<String, String>> ite = universityLocation.entrySet().iterator();
		while(ite.hasNext()){
			Entry<String, String> entry = ite.next();
			bw.write(entry.getKey() + LocationConstants.UNIVERSITY_LOCATION_SPLIT + entry.getValue() + "\n");
		}
		bw.close();
	}

	public  String[] parseLine(String line) {
		if (line == null || line.length() == 0)
			return null;
		String[] strs = line.split(" ");
		String[] result = new String[3];
		int i = 0;
		for (String str : strs) {
			if (str.trim().length() > 0) {
				result[i] = str.trim();
				i++;
				if (i > 2)
					return result;
			}
		}
		return result;
	}

	public  String getLocation(String ip, String country) {
		String location;
		if (country.indexOf("大学") > 0 || country.indexOf("学院") > 0) {
			if(universityLocation.containsKey(country)){
				location = universityLocation.get(country);
			} else {
				String result = WebIp.getAddressByIp(ip).split(" ")[0];
				if(result == null || result.length() == 0)
					System.out.println("error to get area");
				location = parseCountry(result);
				System.out.println(country + " => " + location);
				universityLocation.put(country, location);
			}
		} else {
			location = parseCountry(country);
		}
		return location;
	}
	
	private  String parseCountry(String country){
		for (int i = 0; i < LocationConstants.provinces.length; i++) {
			if (country.indexOf(LocationConstants.provinces[i]) >= 0) {
				String city = country.replace(LocationConstants.provinces[i], "");
				city = LocationParser.parseCity(city);
				if(city.length() == 0)
					city = LocationConstants.OTHER;
				return LocationConstants.COUNTRY_CHINA + LocationConstants.SPLIT + LocationConstants.provinces[i] + LocationConstants.SPLIT + city;
			}
		}
		for (int i = 0; i < LocationConstants.dCity.length; i++) {
			if (country.indexOf(LocationConstants.dCity[i]) >= 0) {
				return LocationConstants.COUNTRY_CHINA + LocationConstants.SPLIT +LocationConstants.dCity[i] + LocationConstants.SPLIT + "其他";
			}
		}
		return LocationConstants.OTHER + LocationConstants.SPLIT + LocationConstants.OTHER + LocationConstants.SPLIT + LocationConstants.OTHER;
	}
}
