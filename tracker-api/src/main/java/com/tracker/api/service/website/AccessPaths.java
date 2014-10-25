package com.tracker.api.service.website;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.api.service.data.WebSiteDataService;
import com.tracker.api.thrift.web.LogFilter;
import com.tracker.api.thrift.web.UserFilter;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.ConfigExt;
import com.tracker.common.utils.StringUtil;
import com.tracker.common.utils.TableRowKeyCompUtil;
import com.tracker.db.dao.webstats.model.WebSiteBaseTableAccess;
import com.tracker.db.dao.webstats.model.WebSiteFieldIndexAccess;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
/**
 * 
 * 文件名：AccessPaths
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:18:26
 * 功能描述：实时浏览记录的服务类
 *
 */
public class AccessPaths {
	private static Logger logger = LoggerFactory.getLogger(AccessPaths.class);
	private List<HbaseCRUD> m_tableList;
	private HbaseCRUD m_baseTable;
	private WebSiteDataService m_dataService;
	
	private static enum FUNC_TYPE{
		user_index,cookie_index,ip_index
	}

	public AccessPaths(String zookeeper) {
		m_dataService = new WebSiteDataService();
		m_baseTable = new HbaseCRUD("log_website",zookeeper);
		m_tableList = new ArrayList<HbaseCRUD>(FUNC_TYPE.values().length);
		for(FUNC_TYPE index : FUNC_TYPE.values()){
			HbaseCRUD tmp = new HbaseCRUD(index.toString(), zookeeper);
			m_tableList.add(index.ordinal(), tmp);
		}
	}
	/**
	 * 
	 * 函数名：getPathsByCookie
	 * 功能描述：对外封装的查询cookie信息函数
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param logFilter
	 * @param date
	 * @param userFilter
	 * @return
	 */
	public PathResult getPathsByCookie(String webId, int startIndex,int count,LogFilter logFilter,String date,
			UserFilter userFilter){
		PathResult tmp = null;
		if(logFilter.getIsCallSELog() == 0)
			tmp = getUserAccessPaths(webId, startIndex, count, logFilter.getVisitType(),date, userFilter, FUNC_TYPE.cookie_index);
		else
			tmp = filterSE(webId, startIndex, count, logFilter.getVisitType(),date, userFilter, FUNC_TYPE.cookie_index);
		return tmp;
	}
	/**
	 * 
	 * 函数名：getPathsByIP
	 * 功能描述：对外封装的查询ip信息的函数
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param logFilter
	 * @param date
	 * @param userFilter
	 * @return
	 */
	public PathResult getPathsByIP(String webId, int startIndex,int count,LogFilter logFilter, String date,
			UserFilter userFilter){
		PathResult tmp = null;
		if(logFilter.getIsCallSELog() == 0)
			tmp = getUserAccessPaths(webId, startIndex, count, logFilter.getVisitType(),date, userFilter, FUNC_TYPE.ip_index);
		else
			tmp = filterSE(webId, startIndex, count, logFilter.getVisitType(),date, userFilter, FUNC_TYPE.ip_index);
		return tmp;
	}
	/**
	 * 
	 * 函数名：getPathsByUser
	 * 功能描述：对外封装的查询user信息的函数
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param logFilter
	 * @param date
	 * @param userFilter
	 * @return
	 */
	public PathResult getPathsByUser(String webId, int startIndex,int count,LogFilter logFilter, String date,
			UserFilter userFilter){
		PathResult tmp = null;
		if(logFilter.getIsCallSELog() == 0)
			tmp = getUserAccessPaths(webId, startIndex, count, logFilter.getVisitType(),date, userFilter, FUNC_TYPE.user_index);
		else
			tmp = filterSE(webId, startIndex, count, logFilter.getVisitType(),date, userFilter, FUNC_TYPE.user_index);
		return tmp;
	}
	/**
	 * 
	 * 函数名：filterSE
	 * 功能描述：功能与getUserAccessPaths一样,但是只取调用了所有引擎的访问.当userFilterde的isCallSELog
	 * 			 字段被设置且指定了userId或者cookieId或者ip时,会取代getUserAccessPaths被调用
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param visitType
	 * @param date
	 * @param userFilter
	 * @param switchType
	 * @return
	 */
	private PathResult filterSE(String webId, int startIndex,int count,int visitType,String date,
			UserFilter userFilter,FUNC_TYPE switchType){
		if(--startIndex < 0 || count < 0){
			return null;
		}
		if(visitType < 2)
			return null;
		HbaseCRUD table = m_tableList.get(switchType.ordinal());
		String key = null;
		Boolean scan_baseTabel = false;
		List<String> others = new ArrayList<String>();
		HbaseParam param = new HbaseParam();
		HbaseResult result = new HbaseResult();
		Integer startPos = startIndex;
		Integer endPos = startIndex + count;
		//switch to different fields
		switch(switchType){
			case cookie_index:
				if(userFilter == null || userFilter.getCookieId() == null){
					scan_baseTabel = true;
				}else
					key = userFilter.getCookieId();
					break;
			case ip_index:
				if(userFilter == null || userFilter.getIp() == null){
					scan_baseTabel = true;
				}else
					key = userFilter.getIp();
					break;
			case user_index:
				if(userFilter == null || userFilter.getUserId() == null){
					scan_baseTabel = true;
				}else
					key = userFilter.getUserId();
					break;
			default:
					return null;
		}
		if(scan_baseTabel){
			//get data from base table
			return null;
		}
		List<Get> gets = new ArrayList<Get>();
		if(userFilter.getUserType() > 0){
			others.add(Integer.toString(userFilter.getUserType()));
		}else{
			Map<Integer,String> tmp = m_dataService.getUserType(Integer.parseInt(webId));
			for(Integer element : tmp.keySet()){
				others.add(element.toString());
			}
		}
		for(String element : others){
			String rowKey = TableRowKeyCompUtil.getPartitionRowKey(key,webId,date,Arrays.asList(element));
			gets.add(new Get(rowKey.getBytes()));
		}
		param.setReadList(gets);
		param.setMaxVersions(Integer.MAX_VALUE);
		List<String> list = new ArrayList<String>();
		table.batchRead(param, result);
		Long totalCount = 0L;
		do{
			list.addAll(WebSiteFieldIndexAccess.getFieldsIndex_Records(result,visitType));
		}while(WebSiteFieldIndexAccess.moveNext(result));
		
		if(list.size() <= 0)
			return null;
		/*--------------------------------------------------------------------------------------------
		 *  	get date from base table
		 * -------------------------------------------------------------------------------------------
		 */
		param = new HbaseParam();
		List<String> inputList = UserVisitLogFields.castToList(PathItem.fields);
		inputList.add(ApacheSearchLog.FIELDS.searchParam.toString());
		param.setColumns(inputList);
		List<Get> mainKeys = new ArrayList<Get>();
		for(String element : list){
			mainKeys.add(new Get(element.getBytes()));
		}
		byte value[] = Bytes.toBytes(true);
		SingleColumnValueFilter seFilter = new SingleColumnValueFilter("infomation".getBytes()
				, UserVisitLogFields.FIELDS.isCallSE.toString().getBytes(),CompareOp.EQUAL ,value);
		seFilter.setFilterIfMissing(true);
		param.addFilter(seFilter);
		param.setReadList(mainKeys);
		m_baseTable.batchRead(param, result);
		PathResult retVal = new PathResult();
		List<PathItem> listPi = getValueList(0, result.size(), result);
		totalCount = (long)listPi.size();
		//sort
		Collections.sort(listPi,new Comparator<PathItem>(){
			@Override
			public int compare(PathItem o1, PathItem o2) {
				// TODO Auto-generated method stub
				if(o2.getVisitTime()  > o1.getVisitTime())
					return 1;
				else if(o2.getVisitTime()  == o1.getVisitTime())
					return 0;
				else
					return -1;
			}
		});
		if(startPos > listPi.size())
			return null;
		if(endPos > listPi.size())
			endPos = listPi.size();
		retVal.setList(listPi.subList(startPos, endPos));
		retVal.setCount(totalCount);
		return retVal;
	}
	/**
	 * 
	 * 函数名：getUserAccessPaths
	 * 功能描述：实时浏览记录实际调用的函数.当未指定userId,cookieId或者ip的key字段时会扫描基础表,取出索引位置
	 * 			 startIndex后的count条记录.否则,根据key值,从索引表中取出该行健下所有的基础表行健,再从基础表中
	 * 			 读取.
	 * @param webId			网站ID
	 * @param startIndex	开始的索引位置
	 * @param count			返回的数量
	 * @param visitType		访问类型
	 * @param date			日期
	 * @param userFilter	查询的过滤,比如Id,用户类型等信息
	 * @param switchType	确定查询的类型,比如user,cookie,或者ip
	 * @return
	 */
	private PathResult getUserAccessPaths(String webId, int startIndex,int count, int visitType,String date,
			UserFilter userFilter,FUNC_TYPE switchType){
		if(--startIndex < 0 || count < 0){
			return null;
		}
		HbaseCRUD table = m_tableList.get(switchType.ordinal());
		String key = null;
		Boolean scan_baseTabel = false;
		List<String> others = new ArrayList<String>();
		HbaseParam param = new HbaseParam();
		HbaseResult result = new HbaseResult();
		Integer startPos = startIndex;
		Integer endPos = startIndex + count;
		//switch to different fields
		switch(switchType){
			case cookie_index:
				if(userFilter == null || userFilter.getCookieId() == null){
					scan_baseTabel = true;
				}else
					key = userFilter.getCookieId();
					break;
			case ip_index:
				if(userFilter == null || userFilter.getIp() == null){
					scan_baseTabel = true;
				}else
					key = userFilter.getIp();
					break;
			case user_index:
				if(userFilter == null || userFilter.getUserId() == null){
					scan_baseTabel = true;
				}else
					key = userFilter.getUserId();
					break;
			default:
					return null;
		}
		if(scan_baseTabel){
			//get data from base table
			return getBaseTable(startPos, endPos,userFilter,date);
		}
		List<Get> gets = new ArrayList<Get>();
		if(userFilter.getUserType() > 0){
			others.add(Integer.toString(userFilter.getUserType()));
		}else{
			Map<Integer,String> tmp = m_dataService.getUserType(Integer.parseInt(webId));
			for(Integer element : tmp.keySet()){
				others.add(element.toString());
			}
		}
		for(String element : others){
			String rowKey = TableRowKeyCompUtil.getPartitionRowKey(key,webId,date,Arrays.asList(element));
			gets.add(new Get(rowKey.getBytes()));
		}
		if( visitType > 0){
			param.setColumns(Arrays.asList(UserVisitLogFields.Index_Family + ":" +
					UserVisitLogFields.INDEX_FIELDS.keyList.toString() + "_" + visitType 
					,UserVisitLogFields.Index_InfoFam.toString() + ":"));
		}
		param.setReadList(gets);
		param.setMaxVersions(endPos);
		List<String> list = new ArrayList<String>();
		table.batchRead(param, result);
		Long totalCount = 0L;
		do{
			list.addAll(WebSiteFieldIndexAccess.getFieldsIndex_Records(result,visitType));
			totalCount += WebSiteFieldIndexAccess.getFieldsIndexCount(result,visitType);
		}while(WebSiteFieldIndexAccess.moveNext(result));
		
		if(list.size() <= 0)
			return null;
		/*--------------------------------------------------------------------------------------------
		 *  	get date from base table
		 * -------------------------------------------------------------------------------------------
		 */
		param = new HbaseParam();
		List<String> inputList = UserVisitLogFields.castToList(PathItem.fields);
		inputList.add(ApacheSearchLog.FIELDS.searchParam.toString());
		param.setColumns(inputList);
		List<Get> mainKeys = new ArrayList<Get>();
		for(String element : list){
			mainKeys.add(new Get(element.getBytes()));
		}
		param.setReadList(mainKeys);
		m_baseTable.batchRead(param, result);
		PathResult retVal = new PathResult();
		List<PathItem> listPi = getValueList(0, result.size(), result);
		//sort
		Collections.sort(listPi,new Comparator<PathItem>(){
			@Override
			public int compare(PathItem o1, PathItem o2) {
				// TODO Auto-generated method stub
				if(o2.getVisitTime()  > o1.getVisitTime())
					return 1;
				else if(o2.getVisitTime()  == o1.getVisitTime())
					return 0;
				else
					return -1;
			}
		});
		if(startPos > listPi.size())
			return null;
		if(endPos > listPi.size())
			endPos = listPi.size();
		retVal.setList(listPi.subList(startPos, endPos));
		retVal.setCount(totalCount);
		return retVal;
	}
	/**
	 * 
	 * 函数名：getBaseTable
	 * 功能描述：从基础表中取第startIndex开始,endIndex结束的数据
	 * @param startIndex
	 * @param endIndex
	 * @param userFilter
	 * @param date
	 * @return
	 */
	private PathResult getBaseTable(Integer startIndex,Integer endIndex,UserFilter userFilter,String date){
		Long totalCount = 0L;
		HbaseParam param = new HbaseParam();
		HbaseResult result = new HbaseResult();
		//add return fields
		List<String> inputList = UserVisitLogFields.castToList(PathItem.fields);
		param.setColumns(inputList);
		//add userType filter
		if(userFilter.getUserType() > 0){
			Integer userType = userFilter.getUserType();
			SingleColumnValueFilter scvf = new SingleColumnValueFilter("infomation".getBytes(),
					FIELDS.userType.toString().getBytes(), CompareOp.EQUAL, Bytes.toBytes(userType));
			param.addFilter(scvf);
		}
		if(date != null ){
			String startDate = date + " 00:00:00";
			String endDate = date + " 24:00:00";
			param.setTimeRange(StringUtil.parseTimeToLong(startDate),
					StringUtil.parseTimeToLong(endDate));
		}
		String nextRow = m_baseTable.readFrom(param, result,null);
		totalCount += result.size();
//		FirstKeyOnlyFilter fkof = new FirstKeyOnlyFilter();
//		while(totalCount < startIndex && nextRow != null){
//			
//			param.addFilter(fkof);
//		}
		while(totalCount < endIndex && nextRow != null){
			HbaseResult tmp = new HbaseResult();
			nextRow = m_baseTable.readFrom(param, tmp, nextRow);
			totalCount += tmp.size();
			result.addList(tmp.list());
		}
		PathResult retVal = new PathResult();
		List<PathItem> listPi = getValueList(startIndex,endIndex,result);
		retVal.setList(listPi);
		retVal.setCount(-1L);
		return retVal;
	}
	/**
	 * 
	 * 函数名：getValueList
	 * 功能描述：从基础表结果集中取出每行数据,放入PathItem中
	 * @param startIndex
	 * @param endIndex
	 * @param result
	 * @return
	 */
	private List<PathItem> getValueList(Integer startIndex,Integer endIndex,HbaseResult result){
		if(result == null || result.size() <= 0 || startIndex >= result.size() || startIndex > endIndex)
			return null;
		List<PathItem> retVal = new ArrayList<AccessPaths.PathItem>();
		if(WebSiteBaseTableAccess.skipTo(result,startIndex)){
			int readCount = 0;
			do{
				PathItem pi = new PathItem();
				List<Object> values = WebSiteBaseTableAccess.getBaseTableItem(result, PathItem.fields);
				if(values.size() > 0){
					for(int i = 0;i < PathItem.fields.length;i++){
						FIELDS field = PathItem.fields[i];
						pi.setField(field,values.get(i));
					}
					retVal.add(pi);
				}
			}while(WebSiteBaseTableAccess.moveNext(result) &&++readCount < (endIndex - startIndex));
		}
		return retVal;
	}
	/**
	 * 
	 * 文件名：PathResult
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月24日 下午1:22:32
	 * 功能描述：实时访问的返回结果类
	 *
	 */
	public static class PathResult {
		private List<PathItem> m_path;
		private Long m_count;

		public PathResult() {
			m_path = new ArrayList<PathItem>();
			m_count = 0L;
		}

		public void setCount(Long count) {
			m_count = count;
		}

		public Long getCount() {
			return m_count;
		}

		public void setList(List<PathItem> pathlist) {
			m_path = new ArrayList<AccessPaths.PathItem>(pathlist);
		}

		public List<PathItem> getList() {
			return m_path;
		}
	}
	/**
	 * 
	 * 文件名：PathItem
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月24日 下午1:22:54
	 * 功能描述：每条访问记录的存储类
	 *
	 */
	public static class PathItem extends UserVisitLogFields {
		
		private static final FIELDS fields[] = { FIELDS.cookieId, FIELDS.webId,FIELDS.totalCount,
				FIELDS.ip, FIELDS.curUrl, FIELDS.visitType,FIELDS.searchParam,FIELDS.responseTime,
				FIELDS.userId,FIELDS.userType,FIELDS.serverLogTime,FIELDS.isCallSE};

		public PathItem(){
			
		}
		
		public boolean getIsCallSE(){
			byte tmp[] = (byte[])getRaw(FIELDS.isCallSE);
			if(tmp != null)
				return Bytes.toBoolean(tmp);
			else
				return false;
		}
		
		public Long getResponseCount(){
			byte tmp[] = (byte[])getRaw(FIELDS.totalCount);
			if(tmp != null && tmp.length == 8)
				return Bytes.toLong(tmp);
			else
				return 0L;
		}
		
		public Integer getResponseTime(){
			byte tmp[] = (byte[])getRaw(FIELDS.responseTime);
			if(tmp != null && tmp.length == 4)
				return Bytes.toInt(tmp);
			else
				return 0;
		}
		
		public String getSearchParam(){
			byte tmp[] = (byte[])getRaw(FIELDS.searchParam);
			if(tmp != null)
				return Bytes.toString(tmp);
			else
				return null;
		}

		public String getUserId() {
			byte tmp[] = (byte[])getRaw(FIELDS.userId);
			if(tmp != null)
				return Bytes.toString(tmp);
			else
				return null;
		}

		public Integer getUserType() {
			byte tmp[] = (byte[])getRaw(FIELDS.userType);
			if(tmp != null && tmp.length == 4)
				return Bytes.toInt(tmp);
			else
				return 0;
		}

		public String getCookieId() {
			byte tmp[] = (byte[])getRaw(FIELDS.cookieId);
			if(tmp != null)
				return Bytes.toString(tmp);
			else
				return null;
		}

		public String getIp() {
			byte tmp[] = (byte[])getRaw(FIELDS.ip);
			if(tmp != null)
				return Bytes.toString(tmp);
			else
				return null;
		}

		public Long getVisitTime() {
			byte tmp[] = (byte[])getRaw(FIELDS.serverLogTime);
			if(tmp != null && tmp.length == 8)
				return Bytes.toLong(tmp);
			else
				return 0L;
		}

		public String getUrl() {
			byte tmp[] = (byte[])getRaw(FIELDS.curUrl);
			if(tmp != null)
				return Bytes.toString(tmp);
			else
				return null;
		}

		public Integer getVisitType() {
			byte tmp[] = (byte[])getRaw(FIELDS.visitType);
			if(tmp != null && tmp.length == 4)
				return Bytes.toInt(tmp);
			else
				return 0;
		}

		public static List<FIELDS> list() {
			return Arrays.asList(fields);
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
		String configFile = java.lang.System.getenv("COMMON_CONFIG");
		Properties properties = ConfigExt.getProperties(hdfsLocation,
				configFile);
		AccessPaths uap = new AccessPaths(
				properties.getProperty("hbase.zookeeper.quorum"));
		UserFilter userFilter = new UserFilter();
		LogFilter logFilter = new LogFilter();
//		logFilter.setIsCallSELog(1);
//		logFilter.setVisitType(2);
//		userFilter.setIp("10.100.50.192");
//		userFilter.setUserType(1);
//		userFilter.userType = 1;
//		userFilter.setCookieId("1409115224555254482");
//		userFilter.setVisitorType(1);
//		userFilter.setCookieId("1409115224555254482");
//		 userFilter.setUserId("276");
//		 userFilter.setUserType(1);
		try {
			int pos =1;
			while(System.in.read() >= 0){
//				PathResult result = uap.getUserAccessPaths("1",1, 100,2,StringUtil.getCurrentDay(),userFilter,FUNC_TYPE.cookie_index);
//				PathResult result = uap.filterSE("1",1, 10,3,StringUtil.getCurrentDay(),userFilter,FUNC_TYPE.cookie_index);
				PathResult result = uap.getPathsByCookie("1",1, 100,logFilter,StringUtil.getCurrentDay(),userFilter);
				pos +=10;
				if(result == null)
					break;
				//test read base table
				 //test cookie
//		PathResult result = uap.getUserAccessPaths("1",1,10, StringUtil.getCurrentDay(),userFilter,FUNC_TYPE.cookie_index);
				 //test userType Filter
//		PathResult result = uap.getAnonymousPaths("1",1,10, StringUtil.getCurrentDay());
//		PathResult result = uap.getHeadHunterPaths(1,10, StringUtil.getCurrentDay());

				if(result == null){
					System.out.println("no data");
					return ;
				}
				System.out.println("totalCount: " + result.getCount() );
				for (PathItem pitem : result.getList()) {
					System.out.println(pitem.getResponseCount() + "\t" + pitem.getResponseTime() + "\t" +
							pitem.getVisitType() + "\t" + pitem.getCookieId() +  "\t" +pitem.getIsCallSE()
							+ "\t" + pitem.getUserId() + "\t" + pitem.getUserType()
							+ "\t" + new Date(Long.valueOf(pitem.getVisitTime()))
							+ "\t" + pitem.getUrl() +  "\t" + pitem.getSearchParam()
							+ "\t" + pitem.getIp());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
