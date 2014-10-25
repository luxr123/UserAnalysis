package com.tracker.common.utils;

import java.io.UnsupportedEncodingException;

public class IPUtil {
	public static byte[] getIpByteArrayFromString(String ip) {
		byte[] ret = new byte[4];
		java.util.StringTokenizer st = new java.util.StringTokenizer(ip, ".");
		try {
			ret[0] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
			ret[1] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
			ret[2] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
			ret[3] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
		} catch (Exception e) {
		}
		return ret;
	}

	public static String getString(byte[] b, int offset, int len, String encoding) {
		try {
			return new String(b, offset, len, encoding);
		} catch (UnsupportedEncodingException e) {
			return new String(b, offset, len);
		}
	}

	public static String getIpStringFromBytes(byte[] ip) {
		StringBuffer sb = new StringBuffer();
		sb.append(ip[0] & 0xFF);
		sb.append('.');
		sb.append(ip[1] & 0xFF);
		sb.append('.');
		sb.append(ip[2] & 0xFF);
		sb.append('.');
		sb.append(ip[3] & 0xFF);
		return sb.toString();
	}
	
	public static synchronized String iplongToIp(long ipaddress) {
		StringBuffer sb = new StringBuffer("");
		sb.append(String.valueOf((ipaddress >>> 24)));
		sb.append(".");
		sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16));
		sb.append(".");
		sb.append(String.valueOf((ipaddress & 0x0000FFFF) >>> 8));
		sb.append(".");
		sb.append(String.valueOf((ipaddress & 0x000000FF)));
		return sb.toString();
	}

	// string ip to long
	public static synchronized long ipStrToLong(String ipaddress) {
		long[] ip = new long[4];
		int position1 = ipaddress.indexOf(".");
		int position2 = ipaddress.indexOf(".", position1 + 1);
		int position3 = ipaddress.indexOf(".", position2 + 1);
		ip[0] = Long.parseLong(ipaddress.substring(0, position1));
		ip[1] = Long.parseLong(ipaddress.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(ipaddress.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(ipaddress.substring(position3 + 1));
		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}
}