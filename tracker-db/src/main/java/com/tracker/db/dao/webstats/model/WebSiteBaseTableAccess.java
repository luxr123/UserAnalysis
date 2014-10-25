package com.tracker.db.dao.webstats.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
/**
 * 
 * 文件名：WebSiteBaseTableAccess
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 下午1:24:21
 * 功能描述：用于从基础表结果集中取出每行纪录信息.
 *
 */
public class WebSiteBaseTableAccess {
	public static Boolean moveNext(HbaseResult result){
		result.modifyCurPos(1);
		return result.getCurPos() < result.size();
	}
	
	public static Boolean skipTo(HbaseResult result,Integer skips){
		result.modifyCurPos(skips);
		return result.getCurPos() < result.size();
	}
	public static List<Object> getBaseTableItem(HbaseResult result,FIELDS fields[]){
		if(result == null || result.size() <= 0)
			return null;
		List<Object> retVal = new ArrayList<Object>();
		for(FIELDS field: fields){
			byte tmp[] = result.getRawValue(result.getCurPos(), "infomation", field.toString());
			retVal.add(tmp);
		}
		return retVal;
	}
	
	public static Object getBaseTableItem(HbaseResult result,String item){
		if(result == null || result.size() <= 0)
			return null;
		return result.getRawValue(result.getCurPos(), "infomation", item);
	}
}
