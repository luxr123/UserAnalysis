package com.tracker.hbase.service.website;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tracker.db.constants.DateType;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.webstats.model.WebSiteCityStats;
import com.tracker.db.dao.webstats.model.WebSiteDateStats;
import com.tracker.db.dao.webstats.model.WebSiteEntryPageStats;
import com.tracker.db.dao.webstats.model.WebSiteHourStats;
import com.tracker.db.dao.webstats.model.WebSitePageStats;
import com.tracker.db.dao.webstats.model.WebSiteProvinceStats;
import com.tracker.db.dao.webstats.model.WebSiteRefTypeStats;
import com.tracker.db.dao.webstats.model.WebSiteSysEnvStats;
import com.tracker.db.dao.webstats.model.WebSiteUserStats;
import com.tracker.db.simplehbase.request.PutRequest;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.RowUtil;
import com.tracker.hbase.service.util.HbaseUtil;
import com.tracker.hbase.service.util.TimeUtil;

/**
 * 文件名：WebSiteCountImpl
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-20 下午4:51:37
 * 功能描述：WebSiteCount接口实现类
 *
 */
public class WebSiteCountImpl implements WebSiteCount {
	private static final Logger logger = LoggerFactory.getLogger(WebSiteCountImpl.class);
	
	private HBaseDao userStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteUserStats.class);
	private HBaseDao hourStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteHourStats.class);
	private HBaseDao dateStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteDateStats.class);
	private HBaseDao refTypeStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteRefTypeStats.class);
	private HBaseDao provinceStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteProvinceStats.class);
	private HBaseDao cityStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteCityStats.class);
	private HBaseDao pageStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSitePageStats.class);
	private HBaseDao entryPageStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteEntryPageStats.class);
	private HBaseDao sysEnvStatsDao = new HBaseDao(HbaseUtil.getHConnection(),
			WebSiteSysEnvStats.class);
	     
	// 用于进行删除的对象
	private DeleteDataForWebSite delete = new DeleteDataForWebSiteImpl();
	
	
	/**
	 * 函数名：getUnSummableKpiDao
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-20 下午4:58:09
	 * 功能描述：根据timeType获取相应的Dao（不可累加指标）
	 * @param timeType
	 * @return
	 */
	private UnSummableKpiDao getUnSummableKpiDao(Integer timeType) {
		UnSummableKpiDao unSummableKpiDao = null;
		if (timeType == DateType.WEEK.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtil.getHConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_WEEK_TABLE);
		} else if (timeType == DateType.MONTH.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtil.getHConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_MONTH_TABLE);
		} else if (timeType == DateType.YEAR.getValue()) {
			unSummableKpiDao = new UnSummableKpiHBaseDaoImpl(HbaseUtil.getHConnection(), UnSummableKpiHBaseDaoImpl.UNSUMMABLE_KPI_YEAR_TABLE);
		}
		return unSummableKpiDao;
	}
	
	
	@Override
	public void countWebUserStats(Integer webId, Integer timeType, String time) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebUserStats(webId, timeType, time);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();

			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){
				rows.add(WebSiteUserStats.generateRowPrefix(webId, searchTimeType, timeStr));
		    }
				
			//获取数据   分类累加
			Map<String, WebSiteUserStats> result = new HashMap<String, WebSiteUserStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteUserStats>> list = userStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteUserStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteUserStats> rowResult: list){
				WebSiteUserStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSiteUserStats.USER_TYPE_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_USER, null, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_USER, null, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteUserStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteUserStats>> putRequestList = new ArrayList<PutRequest<WebSiteUserStats>>();
			for(String key: result.keySet()){
				 String row = WebSiteUserStats.generateRow(webId, timeType, time, Integer.parseInt(key));
				 putRequestList.add(new PutRequest<WebSiteUserStats>(row, result.get(key)));
			}
			
			userStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebUserStats error", e);
		}
	}

	@Override
	public void countWebStatsByHour(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsByHour(webId, timeType, time, visitorType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();

			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){
				rows.add(WebSiteHourStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType));
		    }
				
			//获取数据   分类累加
			Map<String, WebSiteHourStats> result = new HashMap<String, WebSiteHourStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteHourStats>> list = hourStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteHourStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteHourStats> rowResult: list){
				WebSiteHourStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(),WebSiteHourStats.HOUR_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_TIME, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_TIME, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteHourStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteHourStats>> putRequestList = new ArrayList<PutRequest<WebSiteHourStats>>();
			for(String key: result.keySet()){
				 String row = WebSiteHourStats.generateRow(webId, timeType, time, visitorType, Integer.parseInt(key));
				 putRequestList.add(new PutRequest<WebSiteHourStats>(row, result.get(key)));
			}
			hourStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsByHour error", e);
		}
	}

	@Override
	public void countWebStats(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStats(webId, timeType, time, visitorType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();

			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSiteDateStats.generateRow(webId, searchTimeType, timeStr, visitorType));
			}
				
			//获取数据   分类累加
			Map<String, WebSiteDateStats> result = new HashMap<String, WebSiteDateStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteDateStats>> list = dateStatsDao.findObjectListAndKey(rows, WebSiteDateStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteDateStats> rowResult: list){
				WebSiteDateStats stats = rowResult.getT();
				
				String key = time;
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_DATE, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_DATE, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteDateStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteDateStats>> putRequestList = new ArrayList<PutRequest<WebSiteDateStats>>();
			for(String key: result.keySet()){
				String row = WebSiteDateStats.generateRow(webId, timeType, time, visitorType);
				putRequestList.add(new PutRequest<WebSiteDateStats>(row, result.get(key)));
			}
			dateStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStats error", e);
		}
	}

	@Override
	public void countWebStatsForRefType(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsForRefType(webId, timeType, time, visitorType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();
			
			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSiteRefTypeStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType));
			}
				
			//获取数据   分类累加
			Map<String, WebSiteRefTypeStats> result = new HashMap<String, WebSiteRefTypeStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteRefTypeStats>> list = refTypeStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteRefTypeStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteRefTypeStats> rowResult: list){
				WebSiteRefTypeStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSiteRefTypeStats.REF_TYPE_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_REF, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteRefTypeStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteRefTypeStats>> putRequestList = new ArrayList<PutRequest<WebSiteRefTypeStats>>();
			for(String key: result.keySet()){
		  		 String row = WebSiteRefTypeStats.generateRow(webId, timeType, time, visitorType, Integer.parseInt(key));
				 putRequestList.add(new PutRequest<WebSiteRefTypeStats>(row, result.get(key)));
			}
			refTypeStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsForRefType error", e);
		}
	}


	@Override
	public void countWebStatsForProvince(Integer webId, Integer timeType,
			String time, Integer visitorType, Integer countryId) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsForProvince(webId, timeType, time, visitorType, countryId);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();
			
			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSiteProvinceStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType, countryId));
			}
				
			//获取数据   分类累加
			Map<String, WebSiteProvinceStats> result = new HashMap<String, WebSiteProvinceStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteProvinceStats>> list = provinceStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteProvinceStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteProvinceStats> rowResult: list){
				WebSiteProvinceStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSiteProvinceStats.PROVINCE_ID_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PROVINCE, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PROVINCE, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteProvinceStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteProvinceStats>> putRequestList = new ArrayList<PutRequest<WebSiteProvinceStats>>();
			for(String key: result.keySet()){
				 String row = WebSiteProvinceStats.generateRow(webId, timeType, time, visitorType, countryId, Integer.parseInt(key));
				 putRequestList.add(new PutRequest<WebSiteProvinceStats>(row, result.get(key)));
			}
			provinceStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsForProvince error", e);
		}
	}

	@Override
	public void countWebStatsForCity(Integer webId, Integer timeType, String time,
			Integer visitorType, Integer countryId, Integer provinceId) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsForCity(webId, timeType, time, visitorType, countryId, provinceId);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();
			
			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSiteCityStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType, countryId, provinceId));
			}
				
			//获取数据   分类累加
			Map<String, WebSiteCityStats> result = new HashMap<String, WebSiteCityStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteCityStats>> list = cityStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteCityStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteCityStats> rowResult: list){
				WebSiteCityStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSiteCityStats.CITY_ID_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_CITY, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_CITY, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteCityStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteCityStats>> putRequestList = new ArrayList<PutRequest<WebSiteCityStats>>();
			for(String key: result.keySet()){
				 String row = WebSiteCityStats.generateRow(webId, timeType, time, visitorType, countryId, provinceId, Integer.parseInt(key)); 
				 putRequestList.add(new PutRequest<WebSiteCityStats>(row, result.get(key)));
			}
			cityStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsForCity error", e);
		}
	}

	@Override
	public void countWebStatsForPage(Integer webId, Integer timeType, String time,
			Integer visitorType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsForPage(webId, timeType, time, visitorType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();
			
			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSitePageStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType));
			}
				
			//获取数据   分类累加
			Map<String, WebSitePageStats> result = new HashMap<String, WebSitePageStats>();
			List<SimpleHbaseDOWithKeyResult<WebSitePageStats>> list = pageStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSitePageStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSitePageStats> rowResult: list){
				WebSitePageStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSitePageStats.PAGE_SIGN_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PAGE, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_PAGE, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSitePageStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSitePageStats>> putRequestList = new ArrayList<PutRequest<WebSitePageStats>>();
			for(String key: result.keySet()){
				 String row = WebSitePageStats.generateRow(webId, timeType, time, visitorType, key);
				 putRequestList.add(new PutRequest<WebSitePageStats>(row, result.get(key)));
			}
			pageStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsForPage error", e);
		}
	}

	@Override
	public void countWebStatsForEntryPage(Integer webId, Integer timeType,
			String time, Integer visitorType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsForEntryPage(webId, timeType, time, visitorType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();
			
			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSiteEntryPageStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType));
			}
				
			//获取数据   分类累加
			Map<String, WebSiteEntryPageStats> result = new HashMap<String, WebSiteEntryPageStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteEntryPageStats>> list = entryPageStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteEntryPageStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteEntryPageStats> rowResult: list){
				WebSiteEntryPageStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSiteEntryPageStats.PAGE_SIGN_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_ENTRY_PAGE, visitorType, new ArrayList<String>(result.keySet()));

			for(String key: result.keySet()){
				WebSiteEntryPageStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteEntryPageStats>> putRequestList = new ArrayList<PutRequest<WebSiteEntryPageStats>>();
			for(String key: result.keySet()){
				 String row = WebSiteEntryPageStats.generateRow(webId, timeType, time, visitorType, key);
				 putRequestList.add(new PutRequest<WebSiteEntryPageStats>(row, result.get(key)));
			}
			entryPageStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsForEntryPage error", e);
		}
	}

	@Override
	public void countWebStatsForSysEnv(Integer webId, Integer timeType, String time,
			Integer sysType, Integer visitorType) {
		try {
			UnSummableKpiDao unSummableKpiDao = getUnSummableKpiDao(timeType);
			
			//如果有数据则删除
			delete.deleteWebStatsForSysEnv(webId, timeType, time, sysType, visitorType);
			
			List<String> times = TimeUtil.getTimes(timeType, time);
			List<String> rows = new ArrayList<String>();
			
			//生成rowkey
			Integer searchTimeType = DateType.DAY.getValue();
			//如果timeType是年，则查数据时searchTimeType为月
			if (timeType == DateType.YEAR.getValue()) {
				searchTimeType = DateType.MONTH.getValue();
			}
			for(String timeStr: times){ 
				rows.add(WebSiteSysEnvStats.generateRowPrefix(webId, searchTimeType, timeStr, visitorType, sysType));
			}
			
			//获取数据   分类累加
			Map<String, WebSiteSysEnvStats> result = new HashMap<String, WebSiteSysEnvStats>();
			List<SimpleHbaseDOWithKeyResult<WebSiteSysEnvStats>> list = sysEnvStatsDao.findObjectListAndKeyByRowPrefixList(rows, WebSiteSysEnvStats.class, null);
			
			for(SimpleHbaseDOWithKeyResult<WebSiteSysEnvStats> rowResult: list){
				WebSiteSysEnvStats stats = rowResult.getT();
				
				String key = RowUtil.getRowField(rowResult.getRowKey(), WebSiteSysEnvStats.NAME_INDEX);
				if(result.containsKey(key)){
					merge(result.get(key), stats);
				} else {
					result.put(key, stats);
				}
			}
			
			//更新不可累加数据
			Map<String, Long> ipMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_IP, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + sysType, visitorType, new ArrayList<String>(result.keySet()));
			Map<String, Long> uvMap = unSummableKpiDao.getWebSiteUnSummableKpi(UnSummableKpiParam.KPI_UV, time, String.valueOf(webId), UnSummableKpiParam.SIGN_WEB_SYS + RowUtil.FIELD_SPLIT + sysType, visitorType, new ArrayList<String>(result.keySet()));
			
			for(String key: result.keySet()){
				WebSiteSysEnvStats stats = result.get(key);
				stats.setUv(uvMap.get(key));
				stats.setIpCount(ipMap.get(key));
			}
			
			// 存储在hbase中
			List<PutRequest<WebSiteSysEnvStats>> putRequestList = new ArrayList<PutRequest<WebSiteSysEnvStats>>();
			for(String key: result.keySet()){
				 String row = WebSiteSysEnvStats.generateRow(webId, timeType, time, visitorType, sysType, key);
				 putRequestList.add(new PutRequest<WebSiteSysEnvStats>(row, result.get(key)));
			}
			sysEnvStatsDao.putObjectList(putRequestList);
		} catch (Exception e) {
			logger.error("countWebStatsForSysEnv error", e);
		}
	}
	
	
	/**
	 * 合并WebSiteUserStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteUserStats result, WebSiteUserStats stats) {
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteHourStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteHourStats result, WebSiteHourStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setVisitTimes(stats.getVisitTimes() + result.getVisitTimes());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteDateStats result, WebSiteDateStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setVisitTimes(stats.getVisitTimes() + result.getVisitTimes());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteCityStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteCityStats result, WebSiteCityStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setVisitTimes(stats.getVisitTimes() + result.getVisitTimes());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteProvinceStats result, WebSiteProvinceStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setVisitTimes(stats.getVisitTimes() + result.getVisitTimes());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteRefTypeStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteRefTypeStats result, WebSiteRefTypeStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setVisitTimes(stats.getVisitTimes() + result.getVisitTimes());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	
	/**
	 * 合并WebSitePageStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSitePageStats result, WebSitePageStats stats) {
		result.setEntryPageCount(stats.getEntryPageCount() + result.getEntryPageCount());
		result.setNextPageCount(stats.getNextPageCount() + result.getNextPageCount());
		result.setTotalStayTime(stats.getTotalStayTime() + result.getTotalStayTime());
		result.setOutPageCount(stats.getOutPageCount() + result.getOutPageCount());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteEntryPageStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteEntryPageStats result, WebSiteEntryPageStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitPage(stats.getTotalVisitPage() + result.getTotalVisitPage());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
	
	/**
	 * 合并WebSiteEntryPageStats对象
	 * @param result
	 * @param stats
	 */
	public void merge(WebSiteSysEnvStats result, WebSiteSysEnvStats stats) {
		result.setJumpCount(stats.getJumpCount() + result.getJumpCount());
		result.setTotalVisitTime(stats.getTotalVisitTime() + result.getTotalVisitTime());
		result.setIpCount(stats.getIpCount() + result.getIpCount());
		result.setPv(stats.getPv() + result.getPv());
		result.setUv(stats.getUv() + result.getUv());
	}
}
