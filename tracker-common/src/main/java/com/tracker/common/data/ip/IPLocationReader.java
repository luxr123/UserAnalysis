package com.tracker.common.data.ip;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 获取ip地址，目前先调用纯真ip数据库来获取ip对应的地址，并进行一些数据转换
 * 
 * example:
 * IPLocationReader ipReader = new IPLocationReader();
 * LocationEntry location =  ipReader.getLocationEntryByIp("162.87.0.0");
 * System.out.println(location);
 * 
 * @author jason.hua
 *
 */
public class IPLocationReader {
	private static final Logger logger = LoggerFactory.getLogger(IPLocationReader.class);
	private Map<String, String> universityLocation = new HashMap<String, String>();
	private String IP_DATA_NAME = "qqwry.dat";
	private String UNIVERSITY_DATA_NAME = "universityLocation.txt";
	private IPSeeker ipSeeker;
	
	public IPLocationReader(){
		String ipDataPath = IPLocationReader.class.getClassLoader().getResource(IP_DATA_NAME).getPath();//ResourceLoader.getFilePath(IP_DATA_NAME);
		String universityDataPath = IPLocationReader.class.getClassLoader().getResource(UNIVERSITY_DATA_NAME).getPath();//ResourceLoader.getFilePath(UNIVERSITY_DATA_NAME);
		init(ipDataPath, universityDataPath,"");
	}
	
	public IPLocationReader(String ipDataPath, String universityDataPath){
		init(ipDataPath, universityDataPath,"");
	}
	
	public IPLocationReader(String ipDataPath, String universityDataPath,String sHdfsLocation){
		init(ipDataPath, universityDataPath,sHdfsLocation);
	}
	
	/**
	 * 初始化学校与地址对应关系
	 */
	public void init(String ipDataPath, String universityDataPath,String sHdfsLocation){
		ipSeeker = IPSeeker.getInstance();
		BufferedReader br = null;
		try {
			if (sHdfsLocation != "" && sHdfsLocation != null) {
				ipSeeker.Init(ipDataPath, sHdfsLocation);
				FSDataInputStream fsinput = null;
				Configuration conf = new Configuration();
				// parse hdfs server from zookeeper
				Hdfs hdfs;
				FileSystem fs = FileSystem.get(conf);
				hdfs = (Hdfs) AbstractFileSystem.createFileSystem(new URI(
						sHdfsLocation), conf);
				fsinput = hdfs.open(new Path(universityDataPath));
				br = new BufferedReader(new InputStreamReader(fsinput, "utf-8"));
			} else {
				ipSeeker.Init(ipDataPath);
				br = new BufferedReader(new InputStreamReader(
						new FileInputStream(universityDataPath), "utf-8"));
			}
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] strs = line
						.split(LocationConstants.UNIVERSITY_LOCATION_SPLIT);
				if (strs.length == 2) {
					universityLocation.put(strs[0], strs[1]);
				}
			}
		} catch (UnsupportedEncodingException e) {
			logger.error("encode error utf-8 to file: " + universityDataPath, e);
		} catch (IOException e) {
			logger.error("error to read file: " + universityDataPath, e);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("error to close  BufferedReader for file: "
							+ universityDataPath, e);
				}
			}
		}
	}
	
	/**
	 * 通过纯真IP数据库来获取地址，并对地址进行解析和转化，最终格式：国家、省自治区、城市
	 * @param ip
	 * @return
	 */
	public LocationEntry getLocationEntryByIp(String ip){
		LocationEntry entry = new LocationEntry();
		
		String country = ipSeeker.getCountry(ip);
		if (country.indexOf("大学") > 0 || country.indexOf("学院") > 0) {
			String location = getLocationByUniversity(country);
			String[] strs = location.split(LocationConstants.SPLIT);
			entry.setCountry(strs[0]);
			entry.setProvince(strs[1]);
			if(strs.length == 2)
				entry.setCity(strs[1]);
			return entry;
		}
		
		for (int i = 0; i < LocationConstants.provinces.length; i++) {
			if (country.indexOf(LocationConstants.provinces[i]) >= 0) {
				String city = country.replace(LocationConstants.provinces[i], "");
				
				city = LocationParser.parseCity(city);
				if(city.length() == 0)
					city = LocationConstants.OTHER;
				entry.setCity(city);
				entry.setProvince(LocationConstants.provinces[i]);
				entry.setCountry(LocationConstants.COUNTRY_CHINA);
				return entry;
			}
		}
		for (int i = 0; i < LocationConstants.dCity.length; i++) {
			if (country.indexOf(LocationConstants.dCity[i]) >= 0) {
				entry.setCity(LocationConstants.OTHER);
				entry.setProvince(LocationConstants.dCity[i]);
				entry.setCountry(LocationConstants.COUNTRY_CHINA);
				return entry;
			}
		}
		return entry;
	}
	
	public String getLocationByUniversity(String country){
		String location = LocationConstants.OTHER + LocationConstants.SPLIT + LocationConstants.OTHER + LocationConstants.SPLIT + LocationConstants.OTHER;
		if(universityLocation.containsKey(country)){
			location = universityLocation.get(country);
		}
		return location;
	}
}
