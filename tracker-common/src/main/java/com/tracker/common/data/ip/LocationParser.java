package com.tracker.common.data.ip;

public class LocationParser {
	public static String parseCity(String city) {
		if (city.indexOf("市") > -1) {
			city = city.substring(0, city.indexOf("市") + 1);
		}
		if (city.indexOf("地区") > -1
				&& (city.indexOf("县") > -1 || city.indexOf("市") > -1)) {
			city = city.substring(0, city.indexOf("地区") + 2);
		} else if (city.indexOf("州") > -1
				&& (city.indexOf("县") > -1 || city.indexOf("市") > -1)) {
			int index2 = city.indexOf("市");
			if (index2 < 0)
				index2 = city.indexOf("县");
			if (index2 - city.indexOf("州") > 1)
				city = city.substring(0, city.indexOf("州") + 1);
		} else if (city.indexOf("盟") > -1 && (city.indexOf("市") > -1 || city.indexOf("旗") > -1)) {
			city = city.substring(0, city.indexOf("盟") + 1);
		}
		return city;
	}
}
