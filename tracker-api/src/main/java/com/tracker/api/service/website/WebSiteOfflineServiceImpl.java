package com.tracker.api.service.website;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.api.Servers;
import com.tracker.api.service.data.WebSiteDataService;
import com.tracker.api.thrift.web.EntryPageStats;
import com.tracker.api.thrift.web.PageStats;
import com.tracker.api.thrift.web.WebStats;
import com.tracker.api.util.NumericUtil;
import com.tracker.api.util.TimeUtils;
import com.tracker.common.constant.website.ReferrerType;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteCityStats;
import com.tracker.db.dao.webstats.model.WebSiteDateStats;
import com.tracker.db.dao.webstats.model.WebSiteEntryPageStats;
import com.tracker.db.dao.webstats.model.WebSiteHourStats;
import com.tracker.db.dao.webstats.model.WebSiteKeywordStats;
import com.tracker.db.dao.webstats.model.WebSitePageStats;
import com.tracker.db.dao.webstats.model.WebSiteProvinceStats;
import com.tracker.db.dao.webstats.model.WebSiteRefDomainStats;
import com.tracker.db.dao.webstats.model.WebSiteRefTypeStats;
import com.tracker.db.dao.webstats.model.WebSiteSysEnvStats;
import com.tracker.db.dao.webstats.model.WebSiteUserStats;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;


/**
 * 离线统计数据服务（日、周、月、年）
 * 
 * @author jason.hua
 */
public class WebSiteOfflineServiceImpl implements WebSiteService{
	private HBaseDao userStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteUserStats.class);
	private HBaseDao dateStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteDateStats.class);
	private HBaseDao hourStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteHourStats.class);
	private HBaseDao refTypeStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteRefTypeStats.class);
	private HBaseDao refDomainStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteRefDomainStats.class);
	private HBaseDao keywordStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteKeywordStats.class);
	private HBaseDao pageStatsDao = new HBaseDao(Servers.hbaseConnection, WebSitePageStats.class);
	private HBaseDao entryPageStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteEntryPageStats.class);
	private HBaseDao provinceStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteProvinceStats.class);
	private HBaseDao cityStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteCityStats.class);
	private HBaseDao sysEnvStatsDao = new HBaseDao(Servers.hbaseConnection, WebSiteSysEnvStats.class);

	private WebSiteDataService webSiteDataService = new WebSiteDataService();

	/**
	 * 获取基于网站用户类型的统计指标
	 */
	public Map<String, WebStats> getWebUserStats(Integer webId, Integer timeType, String time){
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		List<String> rows = new ArrayList<String>();
		for(Integer userType: webSiteDataService.getUserType(webId).keySet()){
			rows.add(WebSiteUserStats.generateRow(webId, timeType, time, userType));
		}
		List<SimpleHbaseDOWithKeyResult<WebSiteUserStats>> list = userStatsDao.findObjectListAndKey(rows, WebSiteUserStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSiteUserStats> rowResult: list){
			WebSiteUserStats stats = rowResult.getT();
			WebStats webStats = new WebStats();
			if(stats.pv != null ) webStats.setPv(stats.getPv());
			if(stats.uv != null ) webStats.setUv(stats.getUv());
			if(stats.ipCount != null ) webStats.setIpCount(stats.getIpCount());
			int userType = RowUtil.getRowIntField(rowResult.getRowKey(), WebSiteUserStats.USER_TYPE_INDEX);
			result.put(userType + "", webStats); 
		}
		return result;
	}
	
	/**
	 * 获取基于小时段的统计指标
	 */
	public Map<String, WebStats> getWebStatsByHour(Integer webId, Integer timeType, String time, Integer visitorType){
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		final List<String> hours = new ArrayList<String>();
		List<String> rows = new ArrayList<String>();
		for(int i = 0; i < 24; i++){
			hours.add(i + "");
			rows.add(WebSiteHourStats.generateRow(webId, timeType, time, visitorType, i));
		}
		List<SimpleHbaseDOWithKeyResult<WebSiteHourStats>> list = hourStatsDao.findObjectListAndKey(rows, WebSiteHourStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSiteHourStats> rowResult: list){
			WebSiteHourStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String hour = RowUtil.getRowField(rowResult.getRowKey(), WebSiteHourStats.HOUR_INDEX);
			webStats.setShowName(hour); // hour
			webStats.setFieldId(Integer.parseInt(hour));
			result.put(hour, webStats); 
		}
		return result;
	}
	
	/**
	 * 基于时间的统计指标
	 */ 
	public Map<String, WebStats> getWebStatsByDates(Integer webId, Integer timeType, List<String> times, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		//获取rowkey
		List<String> rows = new ArrayList<String>();
		for(String time: times){ 
			rows.add(WebSiteDateStats.generateRow(webId, timeType, time, visitorType));
		}
		List<SimpleHbaseDOWithKeyResult<WebSiteDateStats>> list = dateStatsDao.findObjectListAndKey(rows, WebSiteDateStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSiteDateStats> rowResult: list){
			WebSiteDateStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String date = RowUtil.getRowField(rowResult.getRowKey(), WebSiteDateStats.TIME_INDEX);
			webStats.setShowName(TimeUtils.applyDescForTime(timeType, date));
			result.put(date, webStats); 
		}
		return result;
	}
	
	/**
	 * 获取基于访问来源类型的统计指标
	 */ 
	public Map<String, WebStats> getWebStatsForRefType(Integer webId, Integer timeType, String time, Integer visitorType){
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		//获取rowkey
		List<String> rows = new ArrayList<String>();
		for(ReferrerType refTypeObj: ReferrerType.values()){ 
			rows.add(WebSiteRefTypeStats.generateRow(webId, timeType, time, visitorType, refTypeObj.getValue()));
		}
		
		List<SimpleHbaseDOWithKeyResult<WebSiteRefTypeStats>> list = refTypeStatsDao.findObjectListAndKey(rows, WebSiteRefTypeStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSiteRefTypeStats> rowResult: list){
			WebSiteRefTypeStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String refType = RowUtil.getRowField(rowResult.getRowKey(), WebSiteRefTypeStats.REF_TYPE_INDEX);
			webStats.setShowName(ReferrerType.valueOf(Integer.parseInt(refType)).getName());
			result.put(refType, webStats); 
		}

		return result;
	}
	
	@Override
	public Map<String, WebStats> getWebStatsForRefDomain(Integer webId,
			Integer timeType, String time, Integer refType,
			Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		//获取rowkey
		String rowPrefix = WebSiteRefDomainStats.generateRowPrefix(webId, timeType, time, visitorType, refType);
		QueryExtInfo<WebSiteRefDomainStats> queryExt = new QueryExtInfo<WebSiteRefDomainStats>();
		queryExt.setLimit(0, topNum);
		List<SimpleHbaseDOWithKeyResult<WebSiteRefDomainStats>> list = refDomainStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSiteRefDomainStats.class, queryExt);
		for(SimpleHbaseDOWithKeyResult<WebSiteRefDomainStats> rowResult: list){
			WebSiteRefDomainStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String refDomain = RowUtil.getRowField(rowResult.getRowKey(), WebSiteRefDomainStats.REF_DOMAIN_INDEX);
			webStats.setShowName(refDomain);
			result.put(refDomain, webStats); 
		}
		return result;
	}
	
	@Override
	public Map<String, WebStats> getWebStatsForKeyword(Integer webId,
			Integer timeType, String time, String seDomain,
			Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		String rowPrefix = WebSiteKeywordStats.generateRowPrefix(webId, timeType, time, visitorType, seDomain);

		QueryExtInfo<WebSiteKeywordStats> queryExt = new QueryExtInfo<WebSiteKeywordStats>();
		queryExt.setLimit(0, topNum);
		List<SimpleHbaseDOWithKeyResult<WebSiteKeywordStats>> list = keywordStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSiteKeywordStats.class, queryExt);
		for(SimpleHbaseDOWithKeyResult<WebSiteKeywordStats> rowResult: list){
			WebSiteKeywordStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String refKeyword = RowUtil.getRowField(rowResult.getRowKey(), WebSiteKeywordStats.REF_KEYWORD_INDEX);
			webStats.setShowName(refKeyword);
			result.put(refKeyword, webStats);
		}
		return result;
	}
	
	/**
	 * 基于省份获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForProvince(Integer webId, Integer timeType, String time, Integer visitorType, Integer countryId) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		//获取rowkey
		String rowPrefix = WebSiteProvinceStats.generateRowPrefix(webId, timeType, time, visitorType, countryId);
		
		List<SimpleHbaseDOWithKeyResult<WebSiteProvinceStats>> list = provinceStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSiteProvinceStats.class, null);
		Map<String, WebStats> resultMap = new HashMap<String, WebStats>();
		for(SimpleHbaseDOWithKeyResult<WebSiteProvinceStats> rowResult: list){
			WebSiteProvinceStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String provinceId = RowUtil.getRowField(rowResult.getRowKey(), WebSiteProvinceStats.PROVINCE_ID_INDEX);
			resultMap.put(String.valueOf(provinceId), webStats); 
		}
		return result;
	}
	
	/**
	 * 基于城市获取统计指标
	 */
	public Map<String, WebStats> getWebStatsForCity(Integer webId, Integer timeType,
			String time, Integer countryId, Integer provinceId, Integer visitorType) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		//获取rowkey
		String rowPrefix = WebSiteCityStats.generateRowPrefix(webId, timeType, time, visitorType, countryId, provinceId);
		
		List<SimpleHbaseDOWithKeyResult<WebSiteCityStats>> list = cityStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSiteCityStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSiteCityStats> rowResult: list){
			WebSiteCityStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String cityId = RowUtil.getRowField(rowResult.getRowKey(), WebSiteCityStats.CITY_ID_INDEX);
			result.put(String.valueOf(cityId), webStats); 
		}
		return result;
	}
	
	/**
	 * 获取基于访问页数统计指标
	 */
	public Map<String, PageStats> getWebStatsForPage(Integer webId, Integer timeType, String time, Integer visitorType)  {
		Map<String, PageStats> result = new HashMap<String, PageStats>();
		String rowPrefix = WebSitePageStats.generateRowPrefix(webId, timeType, time, visitorType);
		
		List<SimpleHbaseDOWithKeyResult<WebSitePageStats>> list = pageStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSitePageStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSitePageStats> rowStats: list){
			WebSitePageStats stats = rowStats.getT();
			String row = rowStats.getRowKey();
			PageStats pageStats = new PageStats();
			if(stats.pv != null ) pageStats.setPv(stats.getPv());
			if(stats.uv != null ) pageStats.setUv(stats.getUv());
			if(stats.ipCount != null ) pageStats.setIpCount(stats.getIpCount());
			if(stats.entryPageCount != null ) pageStats.setEntryPageCount(stats.entryPageCount);
			if(stats.nextPageCount != null) pageStats.setNextPageCount(stats.nextPageCount);
			if(stats.outPageCount != null) pageStats.setOutPageCount(stats.outPageCount);
			pageStats.setAvgStayTime(NumericUtil.getRate(stats.totalStayTime,stats.pv));
			pageStats.setOutRate(NumericUtil.getRate(stats.outPageCount,stats.pv));
			String pageSign = RowUtil.getRowField(row,WebSitePageStats.PAGE_SIGN_INDEX);
			result.put(pageSign, pageStats); 
		}
		return result;
	}

	/**
	 * 获取基于入口页的统计指标
	 */
	public Map<String, EntryPageStats> getWebStatsForEntryPage(Integer webId,
			Integer timeType, String time, Integer visitorType) {
		Map<String, EntryPageStats> result = new HashMap<String, EntryPageStats>();
		String rowPrefix = WebSiteEntryPageStats.generateRowPrefix(webId, timeType, time, visitorType);
		
		List<SimpleHbaseDOWithKeyResult<WebSiteEntryPageStats>> list = entryPageStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSiteEntryPageStats.class, null);
		for(SimpleHbaseDOWithKeyResult<WebSiteEntryPageStats> rowStats: list){
			WebSiteEntryPageStats stats = rowStats.getT();
			String row = rowStats.getRowKey();
			EntryPageStats pageStats = new EntryPageStats();
			if(stats.pv != null ) {
				pageStats.setPv(stats.getPv());
				pageStats.setVisitTimes(stats.pv);
			}
			if(stats.uv != null ) pageStats.setUv(stats.getUv());
			if(stats.ipCount != null ) pageStats.setIpCount(stats.getIpCount());
			pageStats.setAvgVisitTime(NumericUtil.getRate(stats.totalVisitTime,stats.pv));
			pageStats.setAvgVisitPage(NumericUtil.getRate(stats.totalVisitPage,stats.pv));
			pageStats.setJumpRate(NumericUtil.getRate(stats.jumpCount,stats.pv));
			String pageSign = RowUtil.getRowField(row,WebSitePageStats.PAGE_SIGN_INDEX);
			result.put(pageSign, pageStats);
		}
		return result;
	}
	
	/**
	 * 获取基于系统环境的统计指标
	 */
	public Map<String, WebStats> getWebStatsForSysEnv(Integer webId, Integer timeType,
			String time, Integer sysType, Integer visitorType, Integer topNum) {
		Map<String, WebStats> result = new HashMap<String, WebStats>();
		String rowPrefix = WebSiteSysEnvStats.generateRowPrefix(webId, timeType, time, visitorType, sysType);
		QueryExtInfo<WebSiteSysEnvStats> query = new QueryExtInfo<WebSiteSysEnvStats>();
		query.setLimit(0, topNum);
		List<SimpleHbaseDOWithKeyResult<WebSiteSysEnvStats>> list = sysEnvStatsDao.findObjectListAndKeyByRowPrefix(rowPrefix, WebSiteSysEnvStats.class, query);

		for(SimpleHbaseDOWithKeyResult<WebSiteSysEnvStats> rowResult: list){
			WebSiteSysEnvStats stats = rowResult.getT();
			WebStats webStats = getWebStats(stats.getPv(), stats.getUv(), stats.getIpCount(), stats.getVisitTimes(), stats.getJumpCount(), stats.getTotalVisitTime());
			String name = RowUtil.getRowField(rowResult.getRowKey(), WebSiteSysEnvStats.NAME_INDEX);
			webStats.setShowName(name);
			result.put(name, webStats);
		}
		return result;
	}
	
	/**
	 * 新访客比率 = 新访客数/访客数
	 * 跳出率 = 跳出次数/访问次数
	 * 平均访问时长 = 总访问时长/访问次数
	 * 平均访问页数 = 浏览量/访问次数
	 */
	private WebStats getWebStats(Long pv, Long uv, Long ipCount, Long visitTimes, Long jumpCount, Long totalVisitTime){
		WebStats webStats = new WebStats();
		webStats.setPv(pv == null? 0: pv);
		webStats.setUv(uv == null? 0: uv);
		webStats.setIpCount(ipCount == null? 0: ipCount);
		webStats.setVisitTimes(visitTimes == null? 0: ipCount);
		webStats.setJumpRate(NumericUtil.getRate(jumpCount, visitTimes));
		webStats.setAvgVisitTime(NumericUtil.getRate(totalVisitTime, visitTimes));
		webStats.setAvgVisitPage(NumericUtil.getRate(pv ,visitTimes));
		return webStats;
	}
}
