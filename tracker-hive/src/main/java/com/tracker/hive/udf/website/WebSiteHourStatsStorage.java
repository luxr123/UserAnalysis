package com.tracker.hive.udf.website;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteHourStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储时段的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class WebSiteHourStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSiteHourStatsStorage.class);
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteHourStats.class);
	private WebSiteHourStats stats = new WebSiteHourStats();

	public int evaluate(Integer dateType, Integer webId, String date, Integer hour, Integer vistorType, List<Long> list) {
		try {
			// 生成row
			String rowKey = WebSiteHourStats.generateRow(webId, UDFUtils.getDateType(dateType), date, vistorType, hour);

			//对象赋值
			stats.setUv(list.get(0));
			stats.setIpCount(list.get(1));
			stats.setPv(list.get(2));
			stats.setVisitTimes(list.get(3));
			stats.setTotalVisitTime(list.get(4));
			stats.setJumpCount(list.get(5));

			//存储在hbase中
			dao.putObject(rowKey, stats);

			return 1;
		} catch (Exception e) {
			logger.error("WebsiteStatsStorage", e);
		}
		
		return 0;
	}
}
