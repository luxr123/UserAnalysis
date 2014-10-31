package com.tracker.storm.drpc.drpcprocess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.EasyPartion;
import com.tracker.common.utils.RequestUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.dao.webstats.model.WebSiteFieldIndexAccess;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
import com.tracker.storm.drpc.drpcresult.DrpcResult;
import com.tracker.storm.drpc.drpcresult.RTVisitorResult;
import com.tracker.storm.drpc.drpcresult.SearchValueResult;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem_hext;
/**
 * 
 * 文件名：RTVisitorProcess
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:13:27
 * 功能描述：实时访客查询时,被SearchRealTimeStatistic调用的处理类.
 *
 */
public class RTVisitorProcess extends HBaseProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7051813751061593425L;
	public static final String ProcessFunc = RequestUtil.RTVisitorReq.RTVISITOR_FUNC;
	private static Logger logger = LoggerFactory.getLogger(RTVisitorProcess.class);
	
	@Override
	public boolean isProcessable(String input) {
		// TODO Auto-generated method stub
		boolean retVal = false;
		
		String splits[]  = input.split(StringUtil.ARUGEMENT_SPLIT);
		if(splits.length >= 7){
			retVal = true;
		}
		return retVal;
	}
	/**
	 * 1对传入的请求调用RequestUtil类的RTVisitorReqde下的parseCookieReq静态方法进行解析,
	 * 返回一个key-value的集合.
	 * 2如果请求指定了userID,cookieId,或者ip,调用getAllValueListByRowkey函数,在没有指定
	 * userType的情况下获取所有的用户类型下的基础表行健,接着对结果集进行排序,根据getKeyWord
	 * 返回的值去重,最后返回请求的endIndex数量.
	 * 3如果未指定userId,cookieId,ip,调用getValueList函数,扫描请求中指定的所有分区,排序后,
	 * 根据getKeyWord的返回值去重,返回请求的endIndex数量.
	 */
	@Override
	public DrpcResult process(String input, Object localbuff) {
		Map<String,Object> request = RequestUtil.RTVisitorReq.parseCookieReq(input);
		Integer startIndex = (Integer) request.get(RequestUtil.STARTINDEX) - 1;
		Integer endIndex  = startIndex + (Integer) request.get(RequestUtil.COUNT) ;
		Integer visitType = (Integer)request.get(UserVisitLogFields.FIELDS.visitType.toString());
		if(startIndex < 0 || endIndex < 0)
			return null;
		String userType = (String)request.get(FIELDS.userType.toString());
		String webId = (String)request.get(FIELDS.webId.toString());
		String key = getKey(request);
		//get time range
		String date = (String) request.get(RequestUtil.DATETIME);
		Long startTime  = parseTimeToLong(date + " 00:00:00");
		Long endTime = parseTimeToLong(date + " 24:00:00");
		int pos = 0;
		List<ValueItem_hext> buf = new ArrayList<ValueItem_hext>();
		if(key != null && !key.equals("")){
			String startRowKey = getStartRowKey(request, webId, key);
			if(startRowKey == null)
				return null;
			List<Integer> userTypes = new ArrayList<Integer>();
			if(userType != null && !userType.equals("") && !userType.equals("0")){
				userTypes.add(Integer.parseInt(userType));
			}else{
				//scan userType
				Map<Integer,String> tmp = dataService.getUserType(Integer.parseInt(webId));
				userTypes.addAll(tmp.keySet());
			}
			for(Integer element : userTypes){
				String tmpKey = compositeRowKey(startRowKey, element.toString(), key, date);
				List<ValueItem_hext> tmp = getAllValueListByRowkey(startTime, endTime, tmpKey,visitType);
				buf.addAll(tmp);
			}
		}else{
			//scan partitions
//			String partitions = "";
//			List<String> startRowKey = new ArrayList<String>();
//			int length = 0;
			while(request.containsKey(RequestUtil.PARTITION + pos)){
				//scan for one partition
				String partition = (String)request.get(RequestUtil.PARTITION + pos++);
//				length = partition.length();
				String startRowKey = request.get(FIELDS.webId.toString()) + StringUtil.ARUGEMENT_SPLIT + partition;
//				 startRowKey.add(request.get(FIELDS.webId.toString()) + StringUtil.ARUGEMENT_SPLIT + partition);
				List<ValueItem_hext> tmp = getValueList(startTime, endTime, startRowKey, userType,visitType,endIndex);
				buf.addAll(tmp);
//				partitions += partition;
			}
			//usring regex for search
//			Collections.sort(startRowKey);
//			String pattern = "^" + request.get(FIELDS.webId.toString()) + StringUtil.ARUGEMENT_SPLIT 
//					+ "[" +partitions + "]" + "{" + length + "}" + StringUtil.ARUGEMENT_SPLIT ;
//			List<ValueItem_hext> tmp = getValueList_Regex(startTime, endTime, startRowKey.get(0)
//					,startRowKey.get(startRowKey.size() - 1) ,pattern, userType,visitType);
//			buf.addAll(tmp);

		}
		//sort the result
		try{
			Collections.sort(buf, new Comparator<ValueItem_hext>() {
				@Override
				public int compare(ValueItem_hext o1, ValueItem_hext o2) {
					// TODO Auto-generated method stub
					if(o2.getTimestamp() > o1.getTimestamp())
						return 1;
					else if(o2.getTimestamp() == o1.getTimestamp())
						return 0;
					else
						return -1;
				}
			});
		}catch(Exception e){
			logger.error(e.getMessage());
			logger.error(buf.toString());
		}
		//filter the same key
		Set<String> keySet = new HashSet<String>();
		List<ValueItem_hext> retList = new ArrayList<ValueItem_hext>();
		int position = 0;
		for(ValueItem_hext element: buf){
			String keys[] = element.getName().split(StringUtil.ARUGEMENT_SPLIT);
			String keyWord = getKeyWord(Arrays.asList(keys));
			if(keySet.contains(keyWord)){
				continue;
			}else{
				keySet.add(keyWord);
				if(position < endIndex){
					retList.add(element);
				}
				position++;
			}
		}
		RTVisitorResult retVal = null;
		if(key != null && !key.equals(""))
			retVal = new RTVisitorResult(retList, keySet.size());
		else
			retVal = new RTVisitorResult(retList, -1);
		return retVal;
	}
	
	protected String getKeyWord(List<String> keys){
//		String userId = keys.get(2);
//		if(userId == null || userId.equals(""))
//			return keys.get(1);
//		else
//			return   userId;
		return keys.get(3);
	}
	
	
	protected String getKey(Map<String,Object> request){
		return (String)request.get(RequestUtil.ARGUMENT + 0);
	}
	
	/**
	 * 
	 * 函数名：getValueList
	 * 功能描述：返回索引表中每条记录的最近值
	 * @param startTime 扫描数据的时间戳起始时间
	 * @param endTime   扫描数据的时间戳结束时间
	 * @param startRowKey 扫描数据的起始行健
	 * @param visitType 用于扫描时过滤的访问类型值
	 * @return 返回ValueItem_hext列表.每条记录由基础表的行健与时间戳,访问数量构成
	 * @return
	 */
	protected List<ValueItem_hext> getValueList(Long startTime,Long endTime,String startRowKey
			,String userType,Integer visitType,Integer endIndex){
		List<ValueItem_hext> retVal = new ArrayList<SearchValueResult.ValueItem_hext>();
		HbaseParam param = new HbaseParam();
		HbaseResult hresult = new HbaseResult();
		String nextKey = null;

		//filter user Type: add userType on rowkey
		if(userType != null && !userType.equals("") && !userType.equals("0")){
			startRowKey += StringUtil.ARUGEMENT_SPLIT + userType;
		}
		String endRowKey = startRowKey + ".";
		startRowKey += StringUtil.ARUGEMENT_SPLIT;
		if(startTime != null && endTime != null)
			param.setTimeRange(startTime, endTime);
		do{
			String rowKey  = null;
			Long timeStamp = null;
			Long count = null;
			m_crud.setReturnSize(endIndex);
			nextKey = m_crud.readRange(param, hresult, startRowKey, endRowKey);
			do{
				rowKey = WebSiteFieldIndexAccess.getFieldsIndex_RecordByVisitType(hresult,visitType);
				if(rowKey == null)
					continue;
				timeStamp = WebSiteFieldIndexAccess.getFieldsIndexTimeStamp(hresult,visitType);
				if(timeStamp == null)
					continue;
				count = WebSiteFieldIndexAccess.getFieldsIndexCount(hresult);
				retVal.add(new ValueItem_hext(rowKey, timeStamp,count));
				if(retVal.size() >= endIndex)
					return retVal;
			}while(WebSiteFieldIndexAccess.moveNext(hresult));
			startRowKey = nextKey;
		}while(nextKey != null);
		return retVal;
	}
	
	/**
	 * 
	 * 函数名：getValueList_Regex
	 * 功能描述：返回索引表中每条记录的最近值
	 * @param startTime 扫描数据的时间戳起始时间
	 * @param endTime   扫描数据的时间戳结束时间
	 * @param startRowKey 扫描数据的起始行健
	 * @param visitType 用于扫描时过滤的访问类型值
	 * @return 返回ValueItem_hext列表.每条记录由基础表的行健与时间戳,访问数量构成
	 * @return
	 */
	protected List<ValueItem_hext> getValueList_Regex(Long startTime,Long endTime,String startRowKey,
			String endRowKey,String pattern,String userType,Integer visitType){
		List<ValueItem_hext> retVal = new ArrayList<SearchValueResult.ValueItem_hext>();
		HbaseParam param = new HbaseParam();
		HbaseResult hresult = new HbaseResult();
		String nextKey = null;
		//filter user Type: add userType on rowkey
		if(userType != null && !userType.equals("") && !userType.equals("0")){
			startRowKey += StringUtil.ARUGEMENT_SPLIT + userType;
			endRowKey += StringUtil.ARUGEMENT_SPLIT + userType;
			pattern += StringUtil.ARUGEMENT_SPLIT + userType;
		}
		startRowKey += StringUtil.ARUGEMENT_SPLIT;
		endRowKey += ".";
		pattern += ".*";
		param.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(pattern,Pattern.MULTILINE)));
		if(startTime != null && endTime != null)
			param.setTimeRange(startTime, endTime);
		do{
			String rowKey = null;
			Long timeStamp = null;
			Long count = null;
			nextKey = m_crud.readRange(param, hresult, startRowKey, endRowKey);
			do{
				rowKey = WebSiteFieldIndexAccess.getFieldsIndex_RecordByVisitType(hresult,visitType);
				if(rowKey == null)
					continue;
				timeStamp = WebSiteFieldIndexAccess.getFieldsIndexTimeStamp(hresult,visitType);
				if(timeStamp == null)
					continue;
				count = WebSiteFieldIndexAccess.getFieldsIndexCount(hresult);
				if(rowKey != null && timeStamp != null)
					retVal.add(new ValueItem_hext(rowKey, timeStamp,count));
			}while(WebSiteFieldIndexAccess.moveNext(hresult));
			startRowKey = nextKey;
		}while(nextKey != null);
		
		return retVal;
	}
	/**
	 * 
	 * 函数名：getAllValueListByRowkey
	 * 功能描述：返回索引表每条记录下的所有版本.
	 * @param startTime 扫描数据的时间戳起始时间
	 * @param endTime   扫描数据的时间戳结束时间
	 * @param startRowKey 扫描数据的起始行健
	 * @param visitType 用于扫描时过滤的访问类型值
	 * @return 返回ValueItem_hext列表.每条记录由基础表的行健与时间戳,访问数量构成
	 */
	protected List<ValueItem_hext> getAllValueListByRowkey(Long startTime,Long endTime,
			String startRowKey,Integer visitType){
		List<ValueItem_hext> retVal = new ArrayList<SearchValueResult.ValueItem_hext>();
		HbaseParam param = new HbaseParam();
		HbaseResult hresult = new HbaseResult();
		String nextKey = null;
		//filter user Type: add userType on rowkey
		String endRowKey = startRowKey + ".";
		if(startTime != null && endTime != null)
			param.setTimeRange(startTime, endTime);
		if(visitType != null && visitType > 0){
			param.setColumns(Arrays.asList(UserVisitLogFields.Index_Family + ":" +
					UserVisitLogFields.INDEX_FIELDS.keyList.toString() + "_" + visitType 
					,UserVisitLogFields.Index_InfoFam.toString() + ":"));
		}
		param.setMaxVersions(-1);
		do{
			nextKey = m_crud.readRange(param, hresult, startRowKey, endRowKey);
			do{
				List<String> rowKey = WebSiteFieldIndexAccess.getFieldsIndex_Records(hresult,visitType);
				List<Long> timeStamp = WebSiteFieldIndexAccess.getFieldsIndexAllTimeStamp(hresult,visitType);
				Long count = WebSiteFieldIndexAccess.getFieldsIndexCount(hresult);
				if(rowKey == null || timeStamp == null || rowKey.size() != timeStamp.size())
					continue;
				int i = 0;
				while(i < rowKey.size()){
					retVal.add(new ValueItem_hext(rowKey.get(i), timeStamp.get(i),count));
					i++;
				}
			}while(WebSiteFieldIndexAccess.moveNext(hresult));
			startRowKey = nextKey;
		}while(nextKey != null);
		
		return retVal;
	}
	/**
	 * 
	 * 函数名：getStartRowKey
	 * 功能描述：
	 * @param request 请求的key-value集合
	 * @param webId webid值
	 * @param key userId,ip,或者cookieId
	 * @return Key值的部分行健,用于扫描操作
	 */
	protected String getStartRowKey(Map<String,Object> request,String webId,String key){
		Integer partition = EasyPartion.getPartition(key);
		//compare partition with partition list
		String compPartition = null;
		int pos = 0;
		while(request.containsKey(RequestUtil.PARTITION + pos)){
			compPartition = (String)request.get(RequestUtil.PARTITION + pos++);
			if(Integer.parseInt(compPartition) == partition){
				break;
			}
		}
		if(compPartition == null || Integer.parseInt(compPartition) != partition){
			return null;
		}
		return webId + StringUtil.ARUGEMENT_SPLIT + partition;
	}
	/**
	 * 
	 * 函数名：compositeRowKey
	 * 功能描述：
	 * @param startRowKey
	 * @param userType
	 * @param key
	 * @param date
	 * @return 拼接扫描的行健
	 */
	protected String compositeRowKey(String startRowKey,String userType,String key, String date){
		return startRowKey + StringUtil.ARUGEMENT_SPLIT + userType
				+ StringUtil.ARUGEMENT_SPLIT + key + StringUtil.ARUGEMENT_SPLIT + date;
	}
}
