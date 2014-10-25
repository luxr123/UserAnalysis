package com.tracker.api.service.website;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import com.tracker.api.thrift.web.UserFilter;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.ConfigExt;
import com.tracker.common.utils.RequestUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.common.utils.TableRowKeyCompUtil;
import com.tracker.db.dao.webstats.model.WebSiteBaseTableAccess;
import com.tracker.db.dao.webstats.model.WebSiteFieldIndexAccess;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
/**
 * 
 * 文件名：UserVisitLog
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:58:19
 * 功能描述：实时访客的服务类
 *
 */
public class UserVisitLog {
	private static Logger logger = LoggerFactory.getLogger(UserVisitLog.class);
	private static enum FUNC_TYPE{
		user_index,cookie_index,ip_index
	}
	private List<DRPCClient> m_drpcClients;
	private HbaseCRUD  m_baseTable;
	private HbaseCRUD m_userIndex;
	private HbaseCRUD m_cookieIndex;
	private Properties m_properties;
	private Integer _point;
	private Integer _drpcServers;
	public UserVisitLog(Properties properties) {
		m_properties = properties;
		String zookeeper = m_properties.getProperty("hbase.zookeeper.quorum");
		String drpcHost = m_properties.getProperty("storm.drpc.server");
		String hosts[] = drpcHost.split(",");
		Integer drpcPort  = Integer.parseInt(m_properties.getProperty("storm.drpc.port"));
		m_drpcClients = new ArrayList<DRPCClient>();
		_drpcServers = 0;
		for(int j = 0 ;j<hosts.length ;j++){
			m_drpcClients.add(new DRPCClient(hosts[j], drpcPort));
			_drpcServers++;
		}
		m_baseTable = new HbaseCRUD("log_website", zookeeper);
		m_userIndex = new HbaseCRUD("user_index",zookeeper);
		m_cookieIndex = new HbaseCRUD("cookie_index",zookeeper);
		_point = 0;
		//for local testbu
//		m_drpcClient = new DRPCClient("10.100.2.92",2000);
	}
	/**
	 * 
	 * 函数名：getLogByUser
	 * 功能描述：用于查询用户实时访问的封装函数
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param visitType
	 * @param date
	 * @param userFilter
	 * @return
	 */
	public VisitResult getLogByUser(String webId, int startIndex, int count,int visitType,String date,
			UserFilter userFilter){
		try {
			VisitResult retVal =  getVisitLog(webId, startIndex, count,visitType, date, userFilter, FUNC_TYPE.user_index);
			updateUserVisitCount(retVal,webId,date);
			return retVal;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 
	 * 函数名：getLogByCookie
	 * 功能描述：用于终端实时访问的封装函数
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param visitType
	 * @param date
	 * @param userFilter
	 * @return
	 */
	public VisitResult getLogByCookie(String webId, int startIndex, int count,int visitType,String date,
			UserFilter userFilter){
		try {
			VisitResult retVal =  getVisitLog(webId, startIndex, count,visitType, date, userFilter, FUNC_TYPE.cookie_index);
			updateCookieVisitCount(retVal,webId,date);
			updateUserVisitCount(retVal,webId,date);
			return retVal;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 
	 * 函数名：getLogByIP
	 * 功能描述：用于ip实时访问查询的封装函数
	 * @param webId
	 * @param startIndex
	 * @param count
	 * @param visitType
	 * @param date
	 * @param userFilter
	 * @return
	 */
	public VisitResult getLogByIP(String webId, int startIndex, int count,int visitType,String date,
			UserFilter userFilter){
		try {
			VisitResult retVal = getVisitLog(webId, startIndex, count,visitType, date, userFilter, FUNC_TYPE.ip_index);
			updateCookieVisitCount(retVal,webId,date);
			updateUserVisitCount(retVal,webId,date);
			return retVal;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 
	 * 函数名：mergeResult
	 * 功能描述：对getVisitLog返回的结果进行重新组合.
	 * @param results
	 * @return
	 */
	private VisitResult mergeResult(List<VisitResult> results){
		VisitResult retVal = null;
		Set<String> keys = new HashSet<String>();
		for(int i = results.size() - 1; i >= 0;i--){
			VisitResult element = results.get(i);
			if(element == null)
				continue;
			if(retVal != null){
				//merge
				for(VisitFields item : element.getVisits()){
					String key = item.getCookieId() + StringUtil.ARUGEMENT_SPLIT + item.getUserId();
					if(keys.contains(key)){
						continue;
					}else{
						keys.add(key);
						retVal.getVisits().add(item);
					}
				}
			}else{
				retVal = element;
				for(VisitFields item : element.getVisits()){
					String key = item.getCookieId() + StringUtil.ARUGEMENT_SPLIT + item.getUserId();
					keys.add(key);
				}
			}
		}
		retVal.setCount(retVal.getVisits().size());
		return retVal;
	}
	/**
	 * 
	 * 函数名：getVisitLog
	 * 功能描述：实时访客请求实际调用的函数.根据输入的user,cookie或者ip请求,构造之后转发给drpc服务器获得
	 * 			 基础表行健,再从基础表中去出信息返回.
	 * @param webId			网站ID
	 * @param startIndex	开始的索引位置
	 * @param count			返回的数量
	 * @param visitType		访问类型
	 * @param date			日期
	 * @param userFilter	查询的过滤,比如Id,用户类型等信息
	 * @param switchType	确定查询的类型,比如user,cookie,或者ip
	 * @return
	 * @throws IOException
	 */
	private VisitResult getVisitLog(String webId, int startIndex, int count,int visitType,String date,
			UserFilter userFilter,FUNC_TYPE switchType) throws IOException {
		if (startIndex < 0 || count < 0 || userFilter == null)
			return null;
		String requestStr = "";
		Integer userType = userFilter.getUserType();
		if(visitType <= 0)
			visitType =0;
		if(userType <= 0)
			userType = 0;
		switch(switchType){
			case cookie_index:
				requestStr = RequestUtil.RTVisitorReq.getCookieReq(webId, visitType,userFilter.getCookieId(),
						userType, startIndex, count, date);
	//			RTVisitorReq.getCookieReq("1",0,0, 1, 10, StringUtil.getCurrentDay()
				break;
			case user_index:
				requestStr = RequestUtil.RTVisitorReq.getUserReq(webId, visitType,userFilter.getUserId(),
						userType, startIndex, count, date);
				break;
			case ip_index:
				requestStr = RequestUtil.RTVisitorReq.getIpReq(webId, visitType, userFilter.getIp(),
						userType, startIndex, count, date);
				break;
			default:
				logger.error("unknow request");
		}
		List<VisitFields> lvf = new ArrayList<UserVisitLog.VisitFields>();
		Integer total = 0;
		//send request
		try {
			String retVal = m_drpcClients.get(_point++%_drpcServers).execute(RequestUtil.DRPC_NAME, requestStr);
			if(retVal == null || retVal.equals("")){
				return null;
			}
			String splits[] = retVal.split(StringUtil.RETURN_ITEM_SPLIT);
			total = Integer.parseInt(splits[0]);
			if(total <= 0)
				return null;
			HbaseParam param = new HbaseParam();
			HbaseResult hresult = new HbaseResult();
			List<String> list = UserVisitLogFields.castToList(VisitFields.fields);
			list.add(ApacheSearchLog.FIELDS.searchParam.toString());
			param.setColumns(list);
			List<Get> gets = new ArrayList<Get>();
			List<Long> countList = new ArrayList<Long>();
			//get date from base table
			//filter the size
			for(int i = startIndex;i< splits.length && i < (startIndex + count);i++){
				String key_count[] = splits[i].split(StringUtil.ARUGEMENT_END);
				gets.add(new Get(key_count[0].getBytes()));
				if(key_count[1] != null && !key_count[1].equals(""))
					countList.add(Long.parseLong(key_count[1]));
				else
					countList.add(null);
			}
			if(gets.size() == 0 )
				return null;
			param.setReadList(gets);
			m_baseTable.batchRead(param, hresult);
			//fill the list
			do{
				VisitFields vf = new VisitFields();
				List<Object> valueList = WebSiteBaseTableAccess.getBaseTableItem(hresult,VisitFields.fields);
				for(int i = 0;i<VisitFields.fields.length;i++){
					FIELDS field = VisitFields.fields[i];
					vf.setField(field,valueList.get(i));
				}
				lvf.add(vf);
			}while(WebSiteBaseTableAccess.moveNext(hresult));

			//set the count
			for(int i = 0;i<lvf.size() && i < countList.size();i++){
				lvf.get(i).setVisitCount(countList.get(i));
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//get count from userIndex
		VisitResult retVal = new VisitResult();
		retVal.setVisit(lvf);
		//get count from redis buff
		retVal.setCount(total);
		return retVal;
	}
	/**
	 * 
	 * 函数名：updateUserVisitCount
	 * 功能描述：在取得实时访问的记录后,如果该记录存在userid,从用户索引表中取出用户的访问数
	 * @param retVal
	 * @param webId
	 * @param date
	 */
	private void updateUserVisitCount(VisitResult retVal,String webId,String date){
		//update userId visit count
		if(retVal == null || webId == null || date == null)
			return;
		List<Get> userIndex = new ArrayList<Get>();
		List<VisitFields> lvf = retVal.getVisits();
		for(int i = 0;i<lvf.size();i++){
			String userId = lvf.get(i).getUserId();
			if(userId != null && !userId.equals("")){
				String userId_index = TableRowKeyCompUtil.getPartitionRowKey(lvf.get(i).getUserId(),
						webId, date, Arrays.asList(lvf.get(i).getUserType().toString()));
				userIndex.add(new Get(userId_index.getBytes()));
			}else{
				userIndex.add(null);
			}
		}
		HbaseResult hresult = new HbaseResult();
		HbaseParam param = new HbaseParam();
		param.setReadList(userIndex);
		m_userIndex.batchRead(param, hresult);
		Long visitTime = null;
		Long count = null;
		for(int j = 0;j<lvf.size();j++){
			String userId = lvf.get(j).getUserId();
			if(userId != null && !userId.equals("")){
				count = WebSiteFieldIndexAccess.getFieldsIndexCount(hresult);
				visitTime = WebSiteFieldIndexAccess.getFieldsIndexVisitTime(hresult);
				lvf.get(j).setVisitCount(count);
				lvf.get(j).setVisitTime(visitTime);
				WebSiteFieldIndexAccess.moveNext(hresult);
			}else{
				continue;
			}
			
		}
	}
	/**
	 * 
	 * 函数名：updateCookieVisitCount
	 * 功能描述：在取得实时访问的记录后,如果该记录不存在userid,那么从cookie索引表中取出用户的访问数
	 * @param retVal
	 * @param webId
	 * @param date
	 */
	private void updateCookieVisitCount(VisitResult retVal,String webId,String date){
		//update userId visit count
		if(retVal == null || webId == null || date == null)
			return;
		List<Get> cookieIndex = new ArrayList<Get>();
		List<VisitFields> lvf = retVal.getVisits();
		for(int i = 0;i<lvf.size();i++){
			String userId = lvf.get(i).getUserId();
			if(userId == null || userId.equals("")){
				String cookie_index = TableRowKeyCompUtil.getPartitionRowKey(lvf.get(i).getCookieId(),
						webId, date, Arrays.asList(lvf.get(i).getUserType().toString()));
				if(cookie_index != null)
					cookieIndex.add(new Get(cookie_index.getBytes()));
				else
					cookieIndex.add(null);
			}
		}
		HbaseResult hresult = new HbaseResult();
		HbaseParam param = new HbaseParam();
		param.setReadList(cookieIndex);
		m_cookieIndex.batchRead(param, hresult);
		for(int j = 0;j<lvf.size();j++){
			String userId = lvf.get(j).getUserId();
			if(userId == null || userId.equals("")){
				Long count = WebSiteFieldIndexAccess.getFieldsIndexCount(hresult);
				Long visitTime = WebSiteFieldIndexAccess.getFieldsIndexVisitTime(hresult);
				lvf.get(j).setVisitCount(count);
				lvf.get(j).setVisitTime(visitTime);
				WebSiteFieldIndexAccess.moveNext(hresult);
			}
		}
	}
	/**
	 * 
	 * 文件名：VisitResult
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月24日 下午12:06:32
	 * 功能描述：实时访客查询的结果类
	 *
	 */
	public static class VisitResult {
		private List<VisitFields> m_visits;
		private int m_count;

		public VisitResult() {
			m_visits = new ArrayList<UserVisitLog.VisitFields>();
			m_count = 0;
		}

		public void setCount(int count) {
			m_count = count;
		}

		public int getCount() {
			return m_count;
		}

		public void setVisit(List<VisitFields> visits) {
			m_visits = visits;
		}

		public List<VisitFields> getVisits() {
			return m_visits;
		}
	}

	/**
	 * 
	 * 文件名：VisitFields
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月24日 下午12:07:51
	 * 功能描述：封装了实时访客页面需要显示的字段
	 *
	 */
	public static class VisitFields extends UserVisitLogFields {
		public static final FIELDS fields[] = { 
				FIELDS.curUrl, FIELDS.cookieId,FIELDS.ip,FIELDS.userId,
				FIELDS.userType,FIELDS.count,FIELDS.visitType,FIELDS.serverLogTime,FIELDS.searchParam};
		private long visit_count;
		private long visit_time;
		public VisitFields(){
			super();
			visit_count = 0L;
		}
		
		public String getSearchParam(){
			byte tmp[] = (byte[])getRaw(FIELDS.searchParam);
			if(tmp != null )
				return Bytes.toString(tmp);
			else
				return null;
		}
		
		public String getCurUrl() {
			byte tmp[] = (byte[])getRaw(FIELDS.curUrl);
			if(tmp != null )
				return Bytes.toString(tmp);
			else
				return null;
		}

		public String getCookieId() {
			byte tmp[] = (byte[])getRaw(FIELDS.cookieId);
			if(tmp != null )
				return Bytes.toString(tmp);
			else
				return null;
		}
		
		public String getIp() {
			byte tmp[] = (byte[])getRaw(FIELDS.ip);
			if(tmp != null )
				return Bytes.toString(tmp);
			else
				return null;
		}
		
		public Long getLogTime() {
			byte tmp[] = (byte[])getRaw(FIELDS.serverLogTime);
			if(tmp != null && tmp.length == 8)
				return Bytes.toLong(tmp);
			else
				return 0L;
		}
		
		public String getUserId(){
			byte tmp[] = (byte[])getRaw(FIELDS.userId);
			if(tmp != null )
				return Bytes.toString(tmp);
			else
				return null;
		}
		
		public Integer getUserType(){
			byte tmp[] = (byte[])getRaw(FIELDS.userType);
			if(tmp != null )
				return Bytes.toInt(tmp);
			else
				return 0;
		}
		
		public Integer getVisitType(){
			byte tmp[] = (byte[])getRaw(FIELDS.visitType);
			if(tmp != null )
				return Bytes.toInt(tmp);
			else
				return 0;
		}

		public Long getVisitCount(){
			return visit_count;
		}
		
		public void setVisitCount(Long count){
			visit_count = count;
		}
		
		public Long getVisitTime(){
			return visit_time;
		}
		
		public void setVisitTime(Long time){
			visit_time = time;
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
		try {
			String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
			String configFile = java.lang.System.getenv("COMMON_CONFIG");
			Properties properties = ConfigExt.getProperties(hdfsLocation, configFile);
			UserVisitLog uvl = new UserVisitLog(properties);
			UserFilter userFilter = new UserFilter();
//			userFilter.setUserType(3);
//			userFilter.setVisitorType(1);
//			userFilter.setUserId("2912");
//			userFilter.setIp("10.100.50.141");
//			userFilter.setVisitorType(0);
//			userFilter.setCookieId("1411002330209737795");
			while(System.in.read() >= 0){
				//test ip cookie user
				VisitResult listUvlf = uvl.getLogByCookie("1", 1, 100,0,StringUtil.getCurrentDay(), userFilter);
//				List<String> list = new ArrayList<String>();
//				list.add("2014-09-03");
//				list.add("2014-09-04");
//				VisitResult listUvlf = uvl.getVisitLog("1", 1, 100,list,userFilter);
				//test user type filter
//				VisitResult listUvlf = uvl.getAnonymousUsers(10, 10, StringUtil.getCurrentDay());
				if(listUvlf == null){
					System.out.println("no data");
					continue;
				}
				System.out.println("total count : " + listUvlf.getCount());
				for (VisitFields uvlf : listUvlf.getVisits()) {
					System.out.print( new Date(uvlf.getLogTime())+ "\t");
					System.out.print(uvlf.getVisitCount() + "\t");
					System.out.print(uvlf.getVisitTime() + "\t");
					System.out.print(uvlf.getCookieId() + "\t");
					System.out.print(uvlf.getVisitType() + "\t");
					System.out.print(uvlf.getUserId() + "\t");
					System.out.print(uvlf.getUserType() + "\t");
					System.out.print(uvlf.getIp() + "\t");
					System.out.print(uvlf.getCurUrl() + "\t");
					System.out.print(uvlf.getSearchParam() + "\t");
					System.out.println();
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
