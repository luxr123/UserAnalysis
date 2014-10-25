package com.tracker.hive.udf.website;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteUserStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储网站概况的用户统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class WebSiteUserStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSiteUserStatsStorage.class);
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteUserStats.class);
	private WebSiteUserStats stats = new WebSiteUserStats();

	public int evaluate(Integer dateType, Integer webId, String date, Integer userType, List<Long> list) {
		try {
			// 生成rowkey
			String row = WebSiteUserStats.generateRow(webId, UDFUtils.getDateType(dateType), date, userType);
			
			// 对象赋值
			stats.setPv(list.get(0));
			stats.setUv(list.get(1));
			stats.setIpCount(list.get(2));

			// 存储在hbase中
			dao.putObject(row, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("WebsiteStatsStorage", e);
		}
		
		return 0;
	}
}
