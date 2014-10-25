package com.tracker.hive.udf.website;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteCityStats;
import com.tracker.db.dao.webstats.model.WebSiteDateStats;
import com.tracker.db.dao.webstats.model.WebSiteProvinceStats;
import com.tracker.db.dao.webstats.model.WebSiteRefDomainStats;
import com.tracker.db.dao.webstats.model.WebSiteRefTypeStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储每日的基本统计信息到hbase中
 * @author xiaorui.lu
 */
@Description(name = "websiteStats", value = "_FUNC_(array<bigint>) - insert into hbase")
public class WebSiteStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSiteStatsStorage.class);

	private HBaseDao cityDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteCityStats.class);
	private HBaseDao provinceDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteProvinceStats.class);
	private HBaseDao dateDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteDateStats.class);
	private HBaseDao refTypeDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteRefTypeStats.class);
	private HBaseDao refDomainDao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteRefDomainStats.class);

	private WebSiteCityStats cityStats;
	private WebSiteProvinceStats provinceStats;
	private WebSiteDateStats dateStats;
	private WebSiteRefTypeStats refTypeStats;
	private WebSiteRefDomainStats refDomainStats;

	public int evaluate(Integer dateType, Integer webId, Integer groupType, String dt, List<Long> list, Map<String, String> map) {
		try {
			//构造rowkey
			String row = generateRow(groupType, webId, UDFUtils.getDateType(dateType), dt, map);
			
			//对象赋值
			switch (groupType) {
				/* 全部（1） */
				case 1:
				/* 访客(2) -> 新/老访客 */
				case 2:
					// 设置kpi指标
					dateStats = new WebSiteDateStats();
					dateStats.setUv(list.get(0));
					dateStats.setIpCount(list.get(1));
					dateStats.setPv(list.get(2));
					dateStats.setVisitTimes(list.get(3));
					dateStats.setTotalVisitTime(list.get(4));
					dateStats.setJumpCount(list.get(5));
					// 存储在hbase中
					dateDao.putObject(row, dateStats);
					break;
				/* 地域(3) */
				case 3:
				/* 访客 + 地域(5) */
				case 5:
					String city = map.get("city_id");
					if (city == null) {
						provinceStats = new WebSiteProvinceStats();
						// 设置kpi指标
						provinceStats.setUv(list.get(0));
						provinceStats.setIpCount(list.get(1));
						provinceStats.setPv(list.get(2));
						provinceStats.setVisitTimes(list.get(3));
						provinceStats.setTotalVisitTime(list.get(4));
						provinceStats.setJumpCount(list.get(5));
	
						provinceDao.putObject(row, provinceStats);
					} else {
						cityStats = new WebSiteCityStats();
						// 设置kpi指标
						cityStats.setUv(list.get(0));
						cityStats.setIpCount(list.get(1));
						cityStats.setPv(list.get(2));
						cityStats.setVisitTimes(list.get(3));
						cityStats.setTotalVisitTime(list.get(4));
						cityStats.setJumpCount(list.get(5));
	
						cityDao.putObject(row, cityStats);
					}
					break;
				/* 来源(4) */
				case 4:
				/* 访客 + 来源(6) */
				case 6:
					String domain = map.get("ref_domain");
					if (domain == null) {
						refTypeStats = new WebSiteRefTypeStats();
						// 设置kpi指标
						refTypeStats.setUv(list.get(0));
						refTypeStats.setIpCount(list.get(1));
						refTypeStats.setPv(list.get(2));
						refTypeStats.setVisitTimes(list.get(3));
						refTypeStats.setTotalVisitTime(list.get(4));
						refTypeStats.setJumpCount(list.get(5));
	
						refTypeDao.putObject(row, refTypeStats);
					} else {
						refDomainStats = new WebSiteRefDomainStats();
						// 设置kpi指标
						refDomainStats.setUv(list.get(0));
						refDomainStats.setIpCount(list.get(1));
						refDomainStats.setPv(list.get(2));
						refDomainStats.setVisitTimes(list.get(3));
						refDomainStats.setTotalVisitTime(list.get(4));
						refDomainStats.setJumpCount(list.get(5));
	
						refDomainDao.putObject(row, refDomainStats);
					}
					break;
				default:
					break;
			}
			
			return 1;
		} catch (Exception e) {
			logger.error("WebsiteStatsStorage", e);
		}
		
		return 0;
	}

	/**
	 * 函数名：generateRow
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-23 下午2:04:27
	 * 功能描述：根据groupType生成rowkey
	 * @param groupType
	 * @param webId
	 * @param dateType
	 * @param date
	 * @param rowkeyMap
	 * @return
	 */
	private String generateRow(int groupType, int webId, int dateType, String date, Map<String, String> rowkeyMap) {
		String visitorType = rowkeyMap.get("vistor_type");
		String city = rowkeyMap.get("city_id");
		String province = rowkeyMap.get("province_id");
		String country = rowkeyMap.get("country_id");
		String refType = rowkeyMap.get("ref_type");
		String domain = rowkeyMap.get("ref_domain");

		Integer visitTypeId = (visitorType == null ? null : Integer.parseInt(visitorType));
		Integer cityId = (city == null ? null : Integer.parseInt(city));
		Integer provinceId = (province == null ? null : Integer.parseInt(province));
		Integer countryId = (country == null ? null : Integer.parseInt(country));
		Integer refTypeId = (refType == null ? null : Integer.parseInt(refType));

		switch (groupType) {
			/* 全部（1） */
			case 1:
				return WebSiteDateStats.generateRow(webId, dateType, date, null);
			/* 访客(2) -> 新/老访客 */
			case 2:
				return WebSiteDateStats.generateRow(webId, dateType, date, visitTypeId);
			/* 地域(3) */
			case 3:
				return getAreaRow(webId, dateType, date, null, cityId, provinceId, countryId);
			/* 来源(4) */
			case 4:
				return getReferer(webId, dateType, date, domain, null, refTypeId);
			/* 访客 + 地域(5) */
			case 5:
				return getAreaRow(webId, dateType, date, visitTypeId, cityId, provinceId, countryId);
			/* 访客 + 来源(6) */
			case 6:
				return getReferer(webId, dateType, date, domain, visitTypeId, refTypeId);
			default:
				return "error";
		}
	}

	/**
	 * 函数名：getReferer
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-23 下午2:15:32
	 * 功能描述：根据domain是否为空分别生成rowkey
	 * @param webId
	 * @param dateType
	 * @param date
	 * @param domain
	 * @param visitTypeId
	 * @param refTypeId
	 * @return
	 */
	private String getReferer(int webId, int dateType, String date, String domain, Integer visitTypeId, Integer refTypeId) {
		if (StringUtils.isNotBlank(domain)) {
			// 1.搜索引擎类型 + 主域名id
			return WebSiteRefDomainStats.generateRow(webId, dateType, date, visitTypeId, refTypeId, domain);
		} else {
			return WebSiteRefTypeStats.generateRow(webId, dateType, date, visitTypeId, refTypeId);
		}
	}

	/**
	 * 函数名：getAreaRow
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-23 下午2:16:23
	 * 功能描述：根据cityId是否为null分别生成rowkey
	 * @param webId
	 * @param dateType
	 * @param date
	 * @param visitTypeId
	 * @param cityId
	 * @param provinceId
	 * @param countryId
	 * @return
	 */
	private String getAreaRow(int webId, int dateType, String date, Integer visitTypeId, Integer cityId, Integer provinceId, Integer countryId) {
		if (cityId != null) {
			// 1. 国家id + 省id + 市id
			return WebSiteCityStats.generateRow(webId, dateType, date, visitTypeId, countryId, provinceId, cityId);
		} else {
			// 2. 国家id + 省id
			return WebSiteProvinceStats.generateRow(webId, dateType, date, visitTypeId, countryId, provinceId);
		}
	}
}
