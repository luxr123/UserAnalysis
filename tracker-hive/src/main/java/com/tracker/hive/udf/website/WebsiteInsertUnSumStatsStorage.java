package com.tracker.hive.udf.website;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tracker.common.constant.website.ReferrerType;
import com.tracker.common.constant.website.SysEnvType;
import com.tracker.db.constants.DateType;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.util.RowUtil;
import com.tracker.hive.Constants;
import com.tracker.hive.db.HiveService;
import com.tracker.hive.udf.UDFUtils;

/**
 * 按照分各维度rowkey格式插入到hbase中; 目的是统计 周,月,年 uv,ip
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年9月19日 下午4:48:48]
 * @Version: [v1.0]
 * 
 */
public class WebsiteInsertUnSumStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebsiteInsertUnSumStatsStorage.class);
	private static Map<Integer, String[]> areaArrayCache = HiveService.getAreaFromIdCache();
	
	/**
	 * 函数名：getUnSummableKpiDao
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-23 下午1:12:08
	 * 功能描述：根据时间类型获取相应的不可累加指标DAO
	 * @param timeType
	 * @return
	 */
	private UnSummableKpiDao getUnSummableKpiDao(Integer dateType) {
		UnSummableKpiDao unSummableKpiDao = null;
		
		int timeType = UDFUtils.getDateType(dateType);
		if (timeType == DateType.WEEK.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_WEEK_TABLE);
		} else if (timeType == DateType.MONTH.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_MONTH_TABLE);
		} else if (timeType == DateType.YEAR.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_YEAR_TABLE);
		}
		
		return unSummableKpiDao;
	}

	public int evaluate(Integer dateType, Integer webId, String date, Integer hour, Integer visitorType, List<String> kpi,
			Map<String, String> map) {
		try {
			List<String> rowPrefixListCookie = new ArrayList<String>();
			List<String> rowPrefixListIp = new ArrayList<String>();
			
			String _webId = String.valueOf(webId);
			
			
			/** 构造rowkey **/
			
			// 日期
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_DATE, UnSummableKpiParam.KPI_UV, date, _webId, null, date, kpi.get(0)));
			    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_DATE, UnSummableKpiParam.KPI_IP, date, _webId, null, date, kpi.get(1)));
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_DATE, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, date, kpi.get(0)));
			    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_DATE, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, date, kpi.get(1)));
			
			// 小时段
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_TIME, UnSummableKpiParam.KPI_UV, date, _webId, null, hour + "", kpi.get(0)));
			    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_TIME, UnSummableKpiParam.KPI_IP, date, _webId, null, hour + "", kpi.get(1)));
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_TIME, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, hour + "", kpi.get(0)));
			    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_TIME, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, hour + "", kpi.get(1)));
			    
			// 来源类型
			Integer refType = Integer.parseInt(map.get("refType"));
			
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF, UnSummableKpiParam.KPI_UV, date, _webId, null, refType + "", kpi.get(0)));
			    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF, UnSummableKpiParam.KPI_IP, date, _webId, null, refType + "", kpi.get(1)));
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, refType + "", kpi.get(0)));
			    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, refType + "", kpi.get(1)));
			
			String refDomain = map.get("refDomain");
			if (refDomain != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, UnSummableKpiParam.KPI_UV, date, _webId, null, refDomain, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, UnSummableKpiParam.KPI_IP, date, _webId, null, refDomain, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, refDomain, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_DOMAIN + RowUtil.FIELD_SPLIT + refType, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, refDomain, kpi.get(1)));
			   
				if (refType == ReferrerType.SEARCH_ENGINE.getValue()) {
					// 外部搜索词
					String refKeyword = map.get("refKeyword");
					if (refKeyword != null) {
						rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + refDomain, UnSummableKpiParam.KPI_UV, date, _webId, null, refKeyword, kpi.get(0)));
						    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + refDomain, UnSummableKpiParam.KPI_IP, date, _webId, null, refKeyword, kpi.get(1)));
						rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + refDomain, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, refKeyword, kpi.get(0)));
						    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_REF_KEYWORD + RowUtil.FIELD_SPLIT + refDomain, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, refKeyword, kpi.get(1)));
					}
				}
			}

			// 页面分析
			String pageSign = map.get("page");
			if (pageSign != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PAGE, UnSummableKpiParam.KPI_UV, date, _webId, null, pageSign, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PAGE, UnSummableKpiParam.KPI_IP, date, _webId, null, pageSign, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PAGE, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, pageSign, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PAGE, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, pageSign, kpi.get(1)));
			}
			
			//入口页面分析
			String sessionPage = map.get("sessionPage");
			if (sessionPage != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, UnSummableKpiParam.KPI_UV, date, _webId, null, sessionPage, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, UnSummableKpiParam.KPI_IP, date, _webId, null, sessionPage, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, sessionPage, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, sessionPage, kpi.get(1)));
			}

			// 地区
			Integer areaId = Integer.parseInt(map.get("areaId"));
			String[] area = areaArrayCache.get(areaId);
			String province = area[0];
			String country = area[1];
			if (province != null && country != null) {
				// 省份
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + country, UnSummableKpiParam.KPI_UV, date, _webId, null, province, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + country, UnSummableKpiParam.KPI_IP, date, _webId, null, province, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + country, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, province, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_PROVINCE + RowUtil.FIELD_SPLIT + country, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, province, kpi.get(1)));
				// 城市
				String city = area[2];
				if (city != null) {
					rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + country + RowUtil.FIELD_SPLIT + province, UnSummableKpiParam.KPI_UV, date, _webId, null, city, kpi.get(0)));
					    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + country + RowUtil.FIELD_SPLIT + province, UnSummableKpiParam.KPI_IP, date, _webId, null, city, kpi.get(1)));
					rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + country + RowUtil.FIELD_SPLIT + province, UnSummableKpiParam.KPI_UV, date, _webId, visitorType, city, kpi.get(0)));
					    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_CITY + RowUtil.FIELD_SPLIT + country + RowUtil.FIELD_SPLIT + province, UnSummableKpiParam.KPI_IP, date, _webId, visitorType, city, kpi.get(1)));
				}
			}

			// 系统环境
			String browser = map.get("browser");
			if (browser != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.BROWSER.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, null, browser, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.BROWSER.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, null, browser, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.BROWSER.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, visitorType, browser, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.BROWSER.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, visitorType, browser, kpi.get(1)));
			}
			String os = map.get("os");
			if (os != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.OS.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, null, os, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.OS.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, null, os, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.OS.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, visitorType, os, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.OS.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, visitorType, os, kpi.get(1)));
			}
			String colorDepth = map.get("colorDepth");
			if (colorDepth != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COLOR_DEPTH.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, null, colorDepth, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COLOR_DEPTH.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, null, colorDepth, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COLOR_DEPTH.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, visitorType, colorDepth, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COLOR_DEPTH.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, visitorType, colorDepth, kpi.get(1)));
			}
			String cookieEnabled = map.get("cookieEnabled");
			if (cookieEnabled != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COOKIE_ENABLED.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, null, cookieEnabled, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COOKIE_ENABLED.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, null, cookieEnabled, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COOKIE_ENABLED.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, visitorType, cookieEnabled, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.COOKIE_ENABLED.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, visitorType, cookieEnabled, kpi.get(1)));
			}
			String language = map.get("language");
			if (language != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.LANGUAGE.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, null, language.toLowerCase(), kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.LANGUAGE.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, null, language.toLowerCase(), kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.LANGUAGE.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, visitorType, language.toLowerCase(), kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.LANGUAGE.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, visitorType, language.toLowerCase(), kpi.get(1)));
			}
			String screen = map.get("screen");
			if (screen != null) {
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.SCREEN.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, null, screen, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.SCREEN.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, null, screen, kpi.get(1)));
				rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.SCREEN.getValue(), UnSummableKpiParam.KPI_UV, date, _webId, visitorType, screen, kpi.get(0)));
				    rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + SysEnvType.SCREEN.getValue(), UnSummableKpiParam.KPI_IP, date, _webId, visitorType, screen, kpi.get(1)));
			}
            
			//存储rowkey集合
			UnSummableKpiDao dao = getUnSummableKpiDao(dateType);
			dao.updateUnSummableKpi(rowPrefixListCookie);
			dao.updateUnSummableKpi(rowPrefixListIp);

			return 1;
		} catch (Exception e) {
			logger.error("WebsiteInsertUnSumStatsStorage", e);
		}
		
		return 0;
	}

	public int evaluate(Integer dateType, Integer webId, String date, Integer userType, List<String> kpi) {
		try {
			List<String> rowPrefixListCookie = new ArrayList<String>();
			List<String> rowPrefixListIp = new ArrayList<String>();
			
			String _webId = String.valueOf(webId);
			
			//构造rowkey
			rowPrefixListCookie.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_USER, UnSummableKpiParam.KPI_UV, date, _webId, null, userType + "", kpi.get(0)));
			rowPrefixListIp.add(UnSummableKpiParam.WebSiteRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_WEB_USER, UnSummableKpiParam.KPI_IP, date, _webId, null, userType + "", kpi.get(1)));

			//存储rowkey集合
			UnSummableKpiDao dao = getUnSummableKpiDao(dateType);
			dao.updateUnSummableKpi(rowPrefixListCookie);
			dao.updateUnSummableKpi(rowPrefixListIp);

			return 1;
		} catch (Exception e) {
			logger.error("WebsiteInsertUnSumStatsStorage", e);
		}
		
		return 0;
	}
}
