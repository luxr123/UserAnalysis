package com.tracker.common.utils;

import java.util.Calendar;
import java.util.List;
/**
 * 
 * 文件名：TableRowKeyCompUtil
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:15:47
 * 功能描述：提供构建hbase表行健的函数
 *
 */
public class TableRowKeyCompUtil {
	public static String getPartitionRowKey(String key,String webId,String date,List<String> appends){
		if(date ==null || key == null || webId == null)
			return null;
		if(date.length() == 8){//YYYYMMDD converts to YYYY-MM-DD
			date = date.substring(0, 4) + "-" +date.substring(4,6) + "-" + date.substring(6,8);
		}
			
		Integer partitionPos = EasyPartion.getPartition(key);
		String partKey = webId + StringUtil.ARUGEMENT_SPLIT + partitionPos ;
		if(appends != null){
			for(String item:appends){
				partKey += StringUtil.ARUGEMENT_SPLIT + item;
			}
		}
		partKey += StringUtil.ARUGEMENT_SPLIT + key + StringUtil.ARUGEMENT_SPLIT + date;
		return partKey;
	}
	
	public static String getPartitionRowKey(String key,String webId,Long dateMillis,List<String> appends){
		if(dateMillis ==null || key == null || webId == null)
			return null;
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(dateMillis);
		String date = cal.get(Calendar.YEAR ) + StringUtil.ARUGEMENT_SPLIT + (cal.get(Calendar.MONTH)+ 1) 
				+ StringUtil.ARUGEMENT_SPLIT + cal.get(Calendar.DAY_OF_MONTH);
		return getPartitionRowKey(key, webId, date, appends);
	}
	
	public static void main(String args[]){
		TableRowKeyCompUtil.getPartitionRowKey("", "1", "20140919", null);
	}
}
