package com.tracker.db.dao.webstats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HConnection;

import com.google.common.collect.Lists;
import com.tracker.common.utils.DateUtils;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.UserSessionData;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;

public class UserSessionHBaseDaoImpl implements UserSessionDao{
	private HBaseDao userSessionTable;
	
	public UserSessionHBaseDaoImpl(HConnection hbaseConnection) {
		userSessionTable = new HBaseDao(hbaseConnection, UserSessionData.class);
	}
	
	@Override
	public void updateUserSession(String row, UserSessionData data) {
		userSessionTable.putObject(row, data);
	}
	
	@Override
	public void updateUserSession(String row, long visitTime, String pageSign) {
		UserSessionData sessionData = new UserSessionData();
		sessionData.setLastPageSign(pageSign);
		sessionData.setLastVisitTime(visitTime);
		userSessionTable.putObject(row, sessionData);

	}

	public UserSessionData getUserSession(String key){
		UserSessionData sessionData = userSessionTable.findObject(key, UserSessionData.class, null);
		return sessionData;
	}
	
	@Override
	public Map<String, UserSessionData> getUserSessions(String ...keys) {
		List<SimpleHbaseDOWithKeyResult<UserSessionData>> sessionDataList = userSessionTable.findObjectListAndKey(Lists.newArrayList(keys), UserSessionData.class, null);
		return userSessionTable.unWrapToMap(sessionDataList);
	}

	@Override
	public void deleteUserSession(String... keys) {
		userSessionTable.deleteRows(Lists.newArrayList(keys));
	}

	@Override
	public List<String> getEndSessionKeys(String rowPrefix, long currentTime, int timeGap, int count) {
		List<String> keys = new ArrayList<String>();
		
		long todayZeroTime = DateUtils.getTodayZeroHour();
		if(todayZeroTime > currentTime - timeGap){
			return keys;
		}
		
		QueryExtInfo<UserSessionData> queryInfo = new QueryExtInfo<UserSessionData>();
		queryInfo.addColumn(UserSessionData.Columns.lastVisitTime.toString());
		queryInfo.setTimeRange(todayZeroTime, currentTime - timeGap);
		queryInfo.setLimit(0, count);
		
	    List<SimpleHbaseDOWithKeyResult<UserSessionData>> list = userSessionTable.findObjectListAndKeyByRowPrefix(rowPrefix, UserSessionData.class, queryInfo);
		
	    for(SimpleHbaseDOWithKeyResult<UserSessionData> rowObj: list){
	    	keys.add(rowObj.getRowKey());
	    }
	    return keys;
	}
	
	public static void main(String[] args) {
		UserSessionDao userSessionDao = new UserSessionHBaseDaoImpl(HbaseUtils.getHConnection("10.100.2.92"));
//		UserSessionData sessionData = userSessionDao.getUserSession(UserSessionData.generateKey("20140923", "1", "1410410219860079134"));
//		System.out.println(sessionData.getLastVisitTime()); 
		System.out.println(userSessionDao.getEndSessionKeys(UserSessionData.generateRowPrefix("20140928"), System.currentTimeMillis(), 30*60*1000, 100));
	}
}
