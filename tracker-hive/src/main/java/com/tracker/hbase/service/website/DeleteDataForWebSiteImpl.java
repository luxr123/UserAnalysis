package com.tracker.hbase.service.website;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteCityStats;
import com.tracker.db.dao.webstats.model.WebSiteDateStats;
import com.tracker.db.dao.webstats.model.WebSiteEntryPageStats;
import com.tracker.db.dao.webstats.model.WebSiteHourStats;
import com.tracker.db.dao.webstats.model.WebSitePageStats;
import com.tracker.db.dao.webstats.model.WebSiteProvinceStats;
import com.tracker.db.dao.webstats.model.WebSiteRefTypeStats;
import com.tracker.db.dao.webstats.model.WebSiteSysEnvStats;
import com.tracker.db.dao.webstats.model.WebSiteUserStats;
import com.tracker.hbase.service.util.HbaseUtil;

/**
 * 
 * 文件名：DeleteDataForWebSiteImpl
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午4:33:42
 * 功能描述：DeleteDataForWebSite接口实现类
 *
 */

public class DeleteDataForWebSiteImpl implements DeleteDataForWebSite {
	private static HBaseDao userStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteUserStats.class);
	private static HBaseDao hourStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteHourStats.class);
	private static HBaseDao dateStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteDateStats.class);
	private static HBaseDao refTypeStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteRefTypeStats.class);
	private static HBaseDao provinceStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteProvinceStats.class);
	private static HBaseDao cityStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteCityStats.class);
	private static HBaseDao pageStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSitePageStats.class);
	private static HBaseDao entryPageStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteEntryPageStats.class);
	private static HBaseDao sysEnvStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteSysEnvStats.class);
	
	@Override
	public void deleteWebUserStats(Integer webId, Integer timeType, String time) {
		userStatsDao.deleteObjectByRowPrefix(WebSiteUserStats.generateRowPrefix(webId, timeType, time));
	}

	@Override
	public void deleteWebStatsByHour(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		hourStatsDao.deleteObjectByRowPrefix(WebSiteHourStats.generateRowPrefix(webId, timeType, time, visitorType)); 
	}

	@Override
	public void deleteWebStats(Integer webId, Integer timeType, String time,
			Integer visitorType) {
		dateStatsDao.deleteObjectByRowPrefix(WebSiteDateStats.generateRow(webId, timeType, time, visitorType));
	}

	@Override
	public void deleteWebStatsForRefType(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		refTypeStatsDao.deleteObjectByRowPrefix(WebSiteRefTypeStats.generateRowPrefix(webId, timeType, time, visitorType)); 
	}

	@Override
	public void deleteWebStatsForProvince(Integer webId, Integer timeType,
			String time, Integer visitorType, Integer countryId) {
		provinceStatsDao.deleteObjectByRowPrefix(WebSiteProvinceStats.generateRowPrefix(webId, timeType, time, visitorType, countryId));  
	}

	@Override
	public void deleteWebStatsForCity(Integer webId, Integer timeType,
			String time, Integer visitorType, Integer countryId,
			Integer provinceId) {
		cityStatsDao.deleteObjectByRowPrefix(WebSiteCityStats.generateRowPrefix(webId, timeType, time, visitorType, countryId, provinceId)); 
	}

	@Override
	public void deleteWebStatsForPage(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		pageStatsDao.deleteObjectByRowPrefix(WebSitePageStats.generateRowPrefix(webId, timeType, time, visitorType));
	}

	@Override
	public void deleteWebStatsForEntryPage(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		entryPageStatsDao.deleteObjectByRowPrefix(WebSiteEntryPageStats.generateRowPrefix(webId, timeType, time, visitorType)); 
	}

	@Override
	public void deleteWebStatsForSysEnv(Integer webId, Integer timeType,
			String time, Integer sysType, Integer visitorType) {
		sysEnvStatsDao.deleteObjectByRowPrefix(WebSiteSysEnvStats.generateRowPrefix(webId, timeType, time, visitorType, sysType));
	}
}
