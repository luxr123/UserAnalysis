package com.tracker.db.dao.webstats.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.INDEX_FIELDS;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
/**
 * 
 * 文件名：WebSiteFieldIndexAccess
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 下午1:27:22
 * 功能描述：用于索引表结果集的存取
 *
 */
public class WebSiteFieldIndexAccess {
	
	/**
	 * 
	 * 函数名：getFieldsIndexValue
	 * 功能描述：在当前指针位置,获取fields字段的最近值,当不指定visitType时且fields=keyList时,那么
	 * 			 visitType将是最近一次访问类型.
	 * @param result
	 * @param family
	 * @param fields
	 * @param visitType
	 * @return
	 */
	private static String getFieldsIndexValue(HbaseResult result,String family,INDEX_FIELDS fields,Integer visitType){
		if(result == null  || result.size() == 0)
			return null;
		String retVal = null;
		byte[] tmp  = null;
		Long value = null;
		String quality = fields.toString();
		switch(fields){
		//add get visitTime operation
		case visitTime:
			tmp = result.getRawValue(result.getCurPos(), family, quality);
			if(tmp == null)
				return null;
			value = Bytes.toLong(tmp);
			if(value != null)
				retVal = value.toString();
			break;
		case count:
			if(visitType != null && visitType > 0)
				quality = fields + "_" + visitType;
			tmp = result.getRawValue(result.getCurPos(), family, quality);
			if(tmp == null)
				return null;
			value= Bytes.toLong(tmp);
			if(value != null)
				retVal = value.toString();
			break;
		case keyList:
			Integer type = null;
			if(visitType ==null || visitType == 0){
				byte tmpArray[] = result.getRawValue(result.getCurPos(), family, INDEX_FIELDS.visitType.toString());
				if(tmpArray != null){
					type = Integer.parseInt(Bytes.toString(tmpArray));
				}else{
					// get the last timeStamp one
					Long compOne = null;
					for(int i = 1; i< 4 ;i++){
						Long ltmp = result.getTimeStamp(result.getCurPos(), family, fields.toString() + "_" + i);
						if(ltmp != null && (compOne == null || ltmp > compOne)){
							compOne = ltmp;
							type = i;
						}
					}
				}
			}else{
				type = visitType;
			}
			if(type == null )
				return null;
			tmp = result.getRawValue(result.getCurPos(), family, fields.toString() + "_" + type);
			if(tmp == null)
				return null;
			retVal = Bytes.toString(tmp);
			break;
			default:
				return null;
		}
		return retVal;
	}
	/**
	 * 
	 * 函数名：getFieldsIndexAllValues
	 * 功能描述：在当前指针的位置,获取fields字段下visitType类型的所有版本值.
	 * @param result
	 * @param fields
	 * @param visitorType
	 * @return
	 */
	private static  List<String> getFieldsIndexAllValues(HbaseResult result,INDEX_FIELDS fields,Integer visitorType){
		if(result == null  || result.size() == 0)
			return null;
		List<String> retVal = new ArrayList<String>();
		retVal.addAll(result.getAllData(result.getCurPos(), UserVisitLogFields.Index_Family,fields.toString() + "_" + visitorType));
		return retVal;
	}
	/**
	 * 
	 * 函数名：moveNext
	 * 功能描述：移动结果集的当前指针
	 * @param result
	 * @return
	 */
	public static Boolean moveNext(HbaseResult result){
		result.modifyCurPos(1);
		return result.getCurPos() < result.size();
	}

	/**
	 * 
	 * 函数名：getFieldsIndexAllTimeStamp
	 * 功能描述：在当前的指针位置,获取所有访问记录的时间戳
	 * @param result
	 * @return
	 */
	public static List<Long> getFieldsIndexAllTimeStamp(HbaseResult result){
		return getFieldsIndexAllTimeStamp(result,null);
	}
	/**
	 * 
	 * 函数名：getFieldsIndexAllTimeStamp
	 * 功能描述：在当前的指针位置,获取所有visitType访问类型的时间戳
	 * @param result
	 * @param visitType
	 * @return
	 */
	public static List<Long> getFieldsIndexAllTimeStamp(HbaseResult result,Integer visitType){
		if(result == null || result.size() == 0)
			return null;
		List<Long> retVal = new ArrayList<Long>();
		List<Integer> visitTypes = new ArrayList<Integer>();
		if(visitType != null && visitType > 0)
			visitTypes.add(visitType);
		else{
			for(int i = 1; i< 4 ;i++){
				visitTypes.add(i);
			}
		}
		for(Integer element:visitTypes){
			retVal.addAll(result.getAllTimeStamp(result.getCurPos(), UserVisitLogFields.Index_Family, 
					INDEX_FIELDS.keyList.toString(),element));
		}
		return retVal;
	}

	/**
	 * 
	 * 函数名：getFieldsIndex_Record
	 * 功能描述：在当前的指针位置,获取索引表最近一次访问记录行健
	 * @param result
	 * @return
	 */
	public static String getFieldsIndex_Record(HbaseResult result){
		return getFieldsIndexValue(result,UserVisitLogFields.Index_Family,INDEX_FIELDS.keyList,null);
	}
	/**
	 * 
	 * 函数名：getFieldsIndex_RecordByVisitType
	 * 功能描述：在当前的指针位置,获取索引表最近一次visitTyp类型的访问记录行健
	 * @param result
	 * @param visitType
	 * @return
	 */
	public static String getFieldsIndex_RecordByVisitType(HbaseResult result,Integer visitType){
		//if visitType is null return the most recently data
		//else return the most recentlyt data by visitType
		return getFieldsIndexValue(result,UserVisitLogFields.Index_Family,INDEX_FIELDS.keyList,visitType);
	}
	/**
	 * 
	 * 函数名：getFieldsIndex_Records
	 * 功能描述：在当前的指针位置,获取所有访问行健
	 * @param result
	 * @return
	 */
	public static List<String> getFieldsIndex_Records(HbaseResult result){
		return getFieldsIndex_Records(result, null);
	}
	/**
	 * 
	 * 函数名：getFieldsIndex_Records
	 * 功能描述：在当前的指针位置,获取visitorType类型的所有访问行健
	 * @param result
	 * @param visitorType
	 * @return
	 */
	public static List<String> getFieldsIndex_Records(HbaseResult result,Integer visitorType){
		if(visitorType != null && visitorType  > 0)
			return getFieldsIndexAllValues(result,INDEX_FIELDS.keyList,visitorType);
		else{
			List<String> retVal = new ArrayList<String>();
			for(int i = 1;i< 4;i++){
				List<String> tmp =getFieldsIndexAllValues(result,INDEX_FIELDS.keyList,i); 
				if(tmp != null)
					retVal.addAll(tmp);
			}
			return retVal;
		}
	}
	/**
	 * 
	 * 函数名：getFieldsIndexCount
	 * 功能描述：在当前的指针位置,获取访问记录的总数
	 * @param result
	 * @return
	 */
	public static Long getFieldsIndexCount(HbaseResult result){
		String str = getFieldsIndexValue(result,UserVisitLogFields.Index_InfoFam,INDEX_FIELDS.count,null);
		if(str != null)
			return Long.parseLong(str);
		else
			return 0L;
	}
	/**
	 * 
	 * 函数名：getFieldsIndexCount
	 * 功能描述：在当前的指针位置,获取visitType访问记录的总数
	 * @param result
	 * @param visitType
	 * @return
	 */
	public static Long getFieldsIndexCount(HbaseResult result,Integer visitType){
		String str = getFieldsIndexValue(result,UserVisitLogFields.Index_InfoFam,INDEX_FIELDS.count,visitType);
		if(str != null)
			return Long.parseLong(str);
		else
			return 0L;
	}
	/**
	 * 
	 * 函数名：getFieldsIndexVisitTime
	 * 功能描述：在当前的指针位置,获取最近一词访问记录的时间
	 * @param result
	 * @return
	 */
	public static Long getFieldsIndexVisitTime(HbaseResult result){
		String str = getFieldsIndexValue(result,UserVisitLogFields.Index_InfoFam,INDEX_FIELDS.visitTime,null);
		if(str != null)
			return Long.parseLong(str);
		else
			return 0L;
	}
	/**
	 * 
	 * 函数名：getFieldsIndexTimeStamp
	 * 功能描述：在当前指针位置,获取最近一次访问visitType访问记录的时间戳
	 * @param result
	 * @param visitType
	 * @return
	 */
	public static Long getFieldsIndexTimeStamp(HbaseResult result,Integer visitType){
		if(result == null || result.size() == 0)
			return null;
		Long retVal = null;
		if(visitType ==null || visitType == 0){
			retVal = result.getTimeStamp(result.getCurPos(), UserVisitLogFields.Index_InfoFam, 
					UserVisitLogFields.INDEX_FIELDS.visitType.toString());
		}else{
			retVal = result.getTimeStamp(result.getCurPos(), UserVisitLogFields.Index_Family, 
					UserVisitLogFields.INDEX_FIELDS.keyList.toString() + "_" +visitType.toString());
		}
		return retVal;
	}
}
	