package com.tracker.storm.kpiStatistic.service.kpi;

import java.util.ArrayList;
import java.util.List;

import com.tracker.common.constant.website.VisitorType;
import com.tracker.common.data.ip.IPLocationReader;
import com.tracker.common.data.ip.LocationEntry;
import com.tracker.common.data.useragent.UserAgentUtil;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.utils.DateUtils;
import com.tracker.db.dao.data.model.Page;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.kpi.model.PageSummableKpi;
import com.tracker.db.dao.kpi.model.WebSiteSummableKpi;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.data.DataService;
import com.tracker.storm.kpiStatistic.service.entity.SummableKpiEntity;
import com.tracker.storm.kpiStatistic.service.entity.WebSiteKpiDimension;

public class WebSiteKpiService {
	private IPLocationReader m_ipReader; //ip转地区对象
	private DataService dataService;//数据服务
	
	public WebSiteKpiService(StormConfig config, boolean isTransIp){
		if(isTransIp)
			m_ipReader =  new IPLocationReader(config.getIpDataPath(), config.getUniversityDataPath(), config.getHdfsPath());
		dataService = new DataService(config.getHbaseConnection());
	}
	
	public SummableKpiEntity computeWebSitePVKpi(WebSiteKpiDimension kpiDimesion){
		SummableKpiEntity kpiEntity = new SummableKpiEntity();
		//更新website的pv指标
		WebSiteSummableKpi kpi = new WebSiteSummableKpi();
		kpi.setPv(1L);
		List<String> webSiteKpiRows = WebSiteKpiRowGenerator.getWebSiteSummableKpiRows( kpiDimesion);
		for(String rowKey: webSiteKpiRows){
			kpiEntity.addWebSiteKpi(rowKey, kpi);
		}
		  
		//更新页面的pv指标
		if(kpiDimesion.getPageSign() != null){
			PageSummableKpi pageKpi = new PageSummableKpi();
			pageKpi.setPv(1L);
			String pageKpiRow = WebSiteKpiRowGenerator.getPageSummableKpiRow(kpiDimesion.getDate(), kpiDimesion.getWebId(), kpiDimesion.getPageSign(), kpiDimesion.getVisitorType());
			kpiEntity.addPageKpi(pageKpiRow, pageKpi);
		}
		return kpiEntity;
	}
	
	public List<String> computeUnSummableKpiKeyForAll(WebSiteKpiDimension kpiDimesion, String ip, String cookieId, String userId){
		 List<String> rowList = new ArrayList<String>();
		//所有维度uv， ip数
		 int userType = kpiDimesion.getUserType();
		 String webId = kpiDimesion.getWebId();
		 String userTypeCookieId = cookieId;
		 if(userId != null && dataService.isLoginUser(Integer.parseInt(webId), userType)){
			 userTypeCookieId = userType + "-" + userId;
		 }
		 
		 //IP
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForAll(UnSummableKpiParam.KPI_IP, kpiDimesion.getVisitorType(), kpiDimesion, ip));
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForAll(UnSummableKpiParam.KPI_IP, null, kpiDimesion, ip));
		 rowList.add(WebSiteKpiRowGenerator.getUnSummableKpiRowsForUser(UnSummableKpiParam.KPI_IP, kpiDimesion.getDate(), null, webId, userType, ip));
		 
		 //UV
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForAll(UnSummableKpiParam.KPI_UV, kpiDimesion.getVisitorType(), kpiDimesion, cookieId));
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForAll(UnSummableKpiParam.KPI_UV, null, kpiDimesion, cookieId));
		 rowList.add(WebSiteKpiRowGenerator.getUnSummableKpiRowsForUser(UnSummableKpiParam.KPI_UV, kpiDimesion.getDate(), null, webId, userType, userTypeCookieId));
		 return rowList;
	}
	
	public List<String> computeUnSummableKpiKeyForBasic(String date, Integer hour, String webId, Integer visitorType, String pageSign, String ip, String cookieId, Integer userType, String userId){
		 List<String> rowList = new ArrayList<String>();

		 String userTypeCookieId = cookieId;
		 if(userId != null && dataService.isLoginUser(Integer.parseInt(webId), userType)){
			 userTypeCookieId = userType + "-" + userId;
		 }
		 //IP
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForBasic(UnSummableKpiParam.KPI_IP, date, visitorType, webId, pageSign, hour, ip));
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForBasic(UnSummableKpiParam.KPI_IP, date, null, webId, pageSign, hour, ip));
		 rowList.add(WebSiteKpiRowGenerator.getUnSummableKpiRowsForUser(UnSummableKpiParam.KPI_IP, date, null, webId, userType, ip));
		 
		 //UV
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForBasic(UnSummableKpiParam.KPI_UV, date, visitorType, webId, pageSign, hour, cookieId));
		 rowList.addAll(WebSiteKpiRowGenerator.getUnSummableKpiRowsForBasic(UnSummableKpiParam.KPI_UV, date, null, webId, pageSign, hour, cookieId));
		 rowList.add(WebSiteKpiRowGenerator.getUnSummableKpiRowsForUser(UnSummableKpiParam.KPI_UV, date, null, webId, userType, userTypeCookieId));
		 return rowList;
	}
	
	public WebSiteKpiDimension getKpiDimension(String logType, Object apacheLogObj){
		WebSiteKpiDimension kpiDimesion = null;
		if(ApachePVLog.APACHE_PV_LOG_TYPE.equalsIgnoreCase(logType)){
			ApachePVLog log = (ApachePVLog)apacheLogObj;
			kpiDimesion = getKpiDimensionByPVLog(log);
		} else if(ApacheSearchLog.APACHE_SEARCH_LOG_TYPE.equalsIgnoreCase(logType)){
			ApacheSearchLog log = (ApacheSearchLog)apacheLogObj;
			kpiDimesion = getKpiDimensionBySearchLog(log);
		}
		return kpiDimesion;
	}
	
	private WebSiteKpiDimension getKpiDimensionByPVLog(ApachePVLog log){
		//获取日期YYYYMMDD
		String date = DateUtils.getDay(log.getServerLogTime());
		//hour
		int hour = DateUtils.getTime(log.getServerLogTime());
		
		//ip转area
		LocationEntry location = m_ipReader.getLocationEntryByIp(log.getIp());
		Integer countryId = dataService.getCountrId(location.getCountry());
		Integer provinceId = dataService.getProvinceId(countryId, location.getProvince());
		Integer cityId = dataService.getCityId(countryId, provinceId, location.getCity());
		
		//生成visitorType
		int visitorType = VisitorType.NEW_VISITOR.getValue();
		if(log.getCookieCreateTime() != null){
			if(!date.equals(DateUtils.getDay(log.getCookieCreateTime()))){
				visitorType = VisitorType.OLD_VISITOR.getValue();
			}
		}
		//转化url为简短标记
		String pageSign = Page.getPageSign(log.getCurUrl());
		String browser = UserAgentUtil.getBrowserByUserAgent(log.getUserAgent());
		String os = UserAgentUtil.getOSByUserAgent(log.getUserAgent());
		
		//创建维度对象
		WebSiteKpiDimension dimension = new WebSiteKpiDimension();
		dimension.setWebId(log.getWebId());
		dimension.setDate(date);
		dimension.setHour(hour);
		dimension.setUserType(log.getUserType());
		dimension.setVisitorType(visitorType);
		dimension.setPageSign(pageSign);
		dimension.setCountryId(countryId);
		dimension.setProvinceId(provinceId);
		dimension.setCityId(cityId);
		dimension.setRefType(log.getRefType());
		dimension.setRefDomain(log.getRefDomain());
		dimension.setRefKeyword(log.getRefKeyword());
		dimension.setColorDepth(log.getColorDepth());
		dimension.setIsCookieEnabled(log.getIsCookieEnabled());
		dimension.setLanguage(log.getLanguage());
		dimension.setScreen(log.getScreen());
		//系统环境
		dimension.setBrowser(browser);
		dimension.setOs(os);
		return dimension;
	}
	
	private WebSiteKpiDimension getKpiDimensionBySearchLog(ApacheSearchLog log){
		//获取日期YYYYMMDD
		String date = DateUtils.getDay(log.getServerLogTime());
		//hour
		int hour = DateUtils.getTime(log.getServerLogTime());
		
		//ip转area
		LocationEntry location = m_ipReader.getLocationEntryByIp(log.getIp());
		Integer countryId = dataService.getCountrId(location.getCountry());
		Integer provinceId = dataService.getProvinceId(countryId, location.getProvince());
		Integer cityId = dataService.getCityId(countryId, provinceId, location.getCity());
		
		//生成visitorType
		int visitorType = VisitorType.NEW_VISITOR.getValue();
		if(log.getCookieCreateTime() != null){
			if(!date.equals(DateUtils.getDay(log.getCookieCreateTime()))){
				visitorType = VisitorType.OLD_VISITOR.getValue();
			}
		}
		//转化url为简短标记
		String pageSign = Page.getPageSign(log.getCurUrl());
		String browser = UserAgentUtil.getBrowserByUserAgent(log.getUserAgent());
		String os = UserAgentUtil.getOSByUserAgent(log.getUserAgent());
		
		//创建维度对象
		WebSiteKpiDimension dimension = new WebSiteKpiDimension();
		dimension.setWebId(log.getWebId());
		dimension.setDate(date);
		dimension.setHour(hour);
		dimension.setUserType(log.getUserType());
		dimension.setVisitorType(visitorType);
		dimension.setPageSign(pageSign);
		dimension.setCountryId(countryId);
		dimension.setProvinceId(provinceId);
		dimension.setCityId(cityId);
		dimension.setRefType(log.getRefType());
		dimension.setColorDepth(log.getColorDepth());
		dimension.setIsCookieEnabled(log.getIsCookieEnabled());
		dimension.setLanguage(log.getLanguage());
		dimension.setScreen(log.getScreen());
		//系统环境
		dimension.setBrowser(browser);
		dimension.setOs(os);
		return dimension;
	}
}
