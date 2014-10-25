package com.tracker.db.dao.webstats;

import java.util.List;
import java.util.Map;

import com.tracker.db.dao.webstats.model.UserSessionData;

/**
 * 用户会话dao类
 * @author jason.hua
 *
 */
public interface UserSessionDao {
	/**
	 * 用户会话记录
	 */
	public void updateUserSession(String rowKey, UserSessionData data);

	public void updateUserSession(String rowKey, long visitTime, String pageSign);
	
	/**
	 * 获取UserSessionData
	 */
	public UserSessionData getUserSession(String key);
	
	public Map<String, UserSessionData> getUserSessions(String ...keys);

	/**
	 * 删除指定用户会话记录
	 */
	public void deleteUserSession(String ...keys);

	/**
	 * 获取{currentTime - timeGap}时间之前的会话（此类会话归为已结束）
	 */
	public List<String> getEndSessionKeys(String rowKeyPrefix, long currentTime, int timeGap, int count);

}
   