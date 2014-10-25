package com.tracker.common.data;

import com.tracker.common.data.ip.IPLocationReader;
import com.tracker.common.data.ip.LocationEntry;

public class IPTest {
	public static void main(String[] args) {
//		IPSeeker ipSeeker = new IPSeeker("qqwry.dat");
//		System.out.println(ipSeeker.getCountry("222.100.10.100"));
		
		IPLocationReader ipReader = new IPLocationReader();
		LocationEntry location =  ipReader.getLocationEntryByIp("116.231.113.212");
		System.out.println(location);
	}
}
