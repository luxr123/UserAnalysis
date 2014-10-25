package com.tracker.common.utils;

import java.util.HashMap;
import java.util.Map;

import com.tracker.common.log.UserVisitLogFields.FIELDS;
/**
 * 
 * 文件名：RequestUtil
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:06:50
 * 功能描述：DRPC请求的封装类,定义请求名称,请求格式,以及格式的解析
 *
 */
public class RequestUtil {
	public static final String DRPC_NAME = "drpc_statistic_server";
	public static final String STARTINDEX= "startIndex";
	public static final String DATETIME= "date";
	public static final String COUNT= "count";
	public static final String PARTITION= "partition";
	public static final String ARGUMENT= "argument";
	
	public static class RTVisitorReq{
		public static final String RTVISITOR_FUNC= "real_time_visitor";
		public static final String RTUSER_FUNC= "real_time_user";
		public static final String RTIP_FUNC= "real_time_ip";
		
		private static String getCommReq(String webId, int visitType, int userType,int startIndex, int count,String date){
			return webId + StringUtil.ARUGEMENT_SPLIT + visitType + StringUtil.ARUGEMENT_SPLIT
					+ userType + StringUtil.ARUGEMENT_SPLIT + startIndex + StringUtil.ARUGEMENT_SPLIT
					+ count + StringUtil.ARUGEMENT_SPLIT+ date;
		}
		
		public static String getCookieReq(String webId, int visitType,String cookie,int userType,int startIndex,
				int count,String date){
			if(webId==null)
				return null;
			if(cookie == null)
				cookie = "";
			return RTVISITOR_FUNC  + StringUtil.ARUGEMENT_SPLIT 
					+ getCommReq(webId, visitType, userType, startIndex, count, date) 
					+StringUtil.ARUGEMENT_SPLIT + cookie + StringUtil.ARUGEMENT_SPLIT
					+StringUtil.ARUGEMENT_END;
			//real_time_visitor-1-null-0-1-100-2014-08-28;
		}
		
		public static String getUserReq(String webId, int visitType,String userId,int userType,int startIndex, 
				int count,String date){
			if(webId==null)
				return null;
			if(userId == null)
				userId = "";
			return RTUSER_FUNC + StringUtil.ARUGEMENT_SPLIT 
					+ getCommReq(webId, visitType, userType, startIndex, count, date) 
					+ StringUtil.ARUGEMENT_SPLIT + userId + StringUtil.ARUGEMENT_SPLIT
					+ StringUtil.ARUGEMENT_END;
			//real_time_visitor-1-null-0-1-100-2014-08-28;
		}
		
		public static String getIpReq(String webId, int visitType,String ip ,int userType,int startIndex, int count,String date){
			if(webId==null)
				return null;
			if(ip == null)
				ip = "";
			return RTIP_FUNC + StringUtil.ARUGEMENT_SPLIT 
					+ getCommReq(webId, visitType,userType, startIndex, count, date) 
					+ StringUtil.ARUGEMENT_SPLIT + ip + StringUtil.ARUGEMENT_SPLIT
					+ StringUtil.ARUGEMENT_END;
			//real_time_visitor-1-null-0-1-100-2014-08-28;
		}
		
		public static Map<String,Object> parseCookieReq(String request){
			String splits[] = request.split(StringUtil.ARUGEMENT_SPLIT);
			if(splits.length < 8){ // the sum of number of common arguments add number of partitons 
				return null;
			}
			Map<String, Object> retVal = new HashMap<String, Object>();
			//parse common arguments
			retVal.put(FIELDS.webId.toString(), splits[1]);
			try{
				retVal.put(FIELDS.visitType.toString(), Integer.parseInt(splits[2]));
			}catch(Exception e){
				retVal.put(FIELDS.visitType.toString(),null);
			}
			retVal.put(FIELDS.userType.toString(), splits[3]);
			try{
				retVal.put(STARTINDEX, Integer.parseInt(splits[4]));
			}catch(Exception e){
				retVal.put(STARTINDEX,null);
			}
			try{
				retVal.put(COUNT, Integer.parseInt(splits[5]));
			}catch(Exception e){
				retVal.put(COUNT, null);
			}
			retVal.put(DATETIME, splits[6] + StringUtil.ARUGEMENT_SPLIT
					+  splits[7] + StringUtil.ARUGEMENT_SPLIT + splits[8]);
			//parse optional arguments
			int tmp = 9;
			int position = 0;
			while(tmp < splits.length && !splits[tmp].equals(StringUtil.ARUGEMENT_END)){
				retVal.put(ARGUMENT + position++, splits[tmp]);
				tmp++;
			}
			tmp++;
			//parse the partitions
			if(tmp < splits.length){
				int pos = 0;
				splits = splits[tmp].split(StringUtil.KEY_VALUE_SPLIT);
				for(int i = 0 ;i < splits.length;i++){
					retVal.put(PARTITION + pos++,splits[i]);
				}
			}
			return retVal;
		}
	}
}
