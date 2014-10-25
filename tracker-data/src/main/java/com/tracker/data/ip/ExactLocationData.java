package com.tracker.data.ip;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tracker.common.data.ip.LocationConstants;

/**
 * 根据data/ip/ipArea.txt中数据，提取地址信息：国家、省、市
 * 用于对比地区解析是否符合规范
 * @author jason.hua
 *
 */
public class ExactLocationData {
	Map<String, List<String>> areaMap = new HashMap<String, List<String>>();
	
	public void exactLocationFromIpDB() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("data/ip/ipArea.txt"), "utf-8"));
		String line = null;
		while((line = br.readLine()) != null){
			String[] strs = line.split(LocationConstants.SPLIT);
			
			//国家、省（直辖市）
			if(strs.length == 4){
				handleLevelByProvince(strs[3]);
			} else if(strs.length == 5){
				handleLevelByCity(strs[3], strs[4]);
			}
		}
		br.close();
		writeToFile("data/ip/location.txt");
	}
	
	public void writeToFile(String fileName) throws IOException{
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
		Iterator<Entry<String, List<String>>> ite = areaMap.entrySet().iterator();
		while(ite.hasNext()){
			Entry<String, List<String>> entry = ite.next();
			List<String> cities = entry.getValue();
			for(String city: cities){
				bw.write("中国"+ LocationConstants.SPLIT + entry.getKey() + LocationConstants.SPLIT + city + "\n");
			}
		}
		bw.close();
	}
	
	public void handleLevelByProvince(String province){
		if(isMunicipality(province)){
			handleLevelByCity(province, province);
		} else {
			handleLevelByCity(province, "其他");
		}
	}
	
	public void handleLevelByCity(String province, String city){
		List<String> list = areaMap.get(province);
		if(list == null){
			list = new ArrayList<String>();
		}
		if(!isContain(list, city)){
			list.add(city);
		}
		areaMap.put(province, list);
	}

	public static boolean isContain(List<String> cities, String city){
		for(String str: cities){
			if(city.equals(str)){
				return true;
			}
		}
		return false;	
	}
	
	public static boolean isMunicipality(String area){
		for (int i = 0; i < LocationConstants.dCity.length; i++) {
			if (area.indexOf(LocationConstants.dCity[i]) >= 0) {
				return true;
			}
		}
		return false;
	}
	
	public static void main(String[] args) throws IOException {
		ExactLocationData areaInit = new ExactLocationData();
		areaInit.exactLocationFromIpDB();
	}
}
