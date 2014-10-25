package com.tracker.db.dao.siteSearch.model;

import com.tracker.common.utils.StringUtil;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

@HBaseTable(tableName = "offline_search_top_ip", defaultFamily = "stats")
public class TopSearchIpStats {
	public static final String SIGN_SE_TOP_IP = "se-top-ip"; //最多搜索次数IP

	public static final int WEB_ID_INDEX = 0;
	public static final int TIME_TYPE_INDEX = 1;
	public static final int TIME_INDEX = 2;
	public static final int SIGN_INDEX = 3;
	public static final int SEARCH_ENGINE_INDEX = 4;
	public static final int SEARCH_TYPE_INDEX = 5;
	public static final int NUM = 6;
	
	@HBaseColumn(qualifier = "ip")
	public String ip;
	
	@HBaseColumn(qualifier = "searchCount")
	public Long searchCount;
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Long getSearchCount() {
		return searchCount;
	}

	public void setSearchCount(Long searchCount) {
		this.searchCount = searchCount;
	}

	public static String generateRow(Integer webId, Integer timeType, String time, Integer searchEngineId, Integer searchType, Integer num){
		Util.checkNull(num);
		return generateRowPrefix(webId, timeType, time, searchEngineId, searchType) + StringUtil.fillLeftData(num+"", 5, '0');
	}
	
	public static String generateRowPrefix(Integer webId, Integer timeType, String time, Integer searchEngineId, Integer searchType){
		Util.checkNull(webId);
		Util.checkNull(timeType);
		Util.checkEmptyString(time);
		Util.checkNull(searchEngineId);
		
		StringBuffer sb = new StringBuffer();
		sb.append(webId).append(RowUtil.ROW_SPLIT);
		sb.append(timeType).append(RowUtil.ROW_SPLIT);
		sb.append(time).append(RowUtil.ROW_SPLIT);
		sb.append(SIGN_SE_TOP_IP).append(RowUtil.ROW_SPLIT);
		sb.append(searchEngineId).append(RowUtil.ROW_SPLIT);
		sb.append(searchType == null? "":searchType).append(RowUtil.ROW_SPLIT);

		return sb.toString();
	}
}
