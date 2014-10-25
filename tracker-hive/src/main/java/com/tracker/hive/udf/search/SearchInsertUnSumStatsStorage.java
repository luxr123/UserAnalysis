package com.tracker.hive.udf.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tracker.db.constants.DateType;
import com.tracker.db.dao.kpi.UnSummableKpiDao;
import com.tracker.db.dao.kpi.UnSummableKpiHBaseDaoImpl;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.db.HiveService;
import com.tracker.hive.udf.UDFUtils;

/**
 * 将cookieId/IP 按照分各维度rowkey格式插入到hbase中; 目的是统计 周,月,年 uv,ip
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年9月19日 下午4:48:48]
 * @Version: [v1.0]
 * 
 */
public class SearchInsertUnSumStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(SearchInsertUnSumStatsStorage.class);

	private static Map<String, Integer> searchEngineMap = HiveService.getSearchEngineCache();
	
	/**
	 * 函数名：getUnSummableKpiDao
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-22 下午3:08:10
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
	
	public int evaluate(Integer dateType, Integer webId, String date, String siteSearchEngine, Integer searchType, List<String> kpi) {
		try {
			List<String> rowPrefixListCookie = new ArrayList<String>();
			List<String> rowPrefixListIp = new ArrayList<String>();
			
			String _webId = String.valueOf(webId);
			
			Integer seId = searchEngineMap.get(siteSearchEngine);
			if(seId == null) {
				seId = -1;
			}
			
			//构造rowkey
			rowPrefixListCookie.add(UnSummableKpiParam.SearchRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_SE_DATE, UnSummableKpiParam.KPI_UV, date, _webId, seId, searchType, kpi.get(0)));
			rowPrefixListIp.add(UnSummableKpiParam.SearchRowGenerator.generateRowKey(UnSummableKpiParam.SIGN_SE_DATE, UnSummableKpiParam.KPI_IP, date, _webId, seId, searchType, kpi.get(1)));
			
			//存储数据
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
