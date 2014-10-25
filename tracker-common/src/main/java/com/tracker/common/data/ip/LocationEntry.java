package com.tracker.common.data.ip;


public class LocationEntry {
	private String country = LocationConstants.OTHER;//中国，其他
	private String province = LocationConstants.OTHER;//省，自治区，直辖市，特别行政区
	private String city = LocationConstants.OTHER;//省,自治区下的城市名
	
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	
	public String toString(){
		return country + "\t" + province + "\t" + city;
	}
}