package com.tracker.hive.udf.website;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteEntryPageStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储入口页面的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
@Description(name = "websiteEntryPageStats", value = "_FUNC_(array<bigint>) - insert into hbase")
public class WebSiteEntryPageStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSiteEntryPageStatsStorage.class);
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteEntryPageStats.class);
	private WebSiteEntryPageStats stats = new WebSiteEntryPageStats();

	public int evaluate(Integer dateType, Integer webId, String dt, Integer visitorType, String pageSign, List<Long> kpi_list) {
		try {
			//生成rowkey
			String rowkey = WebSiteEntryPageStats.generateRow(webId, UDFUtils.getDateType(dateType), dt, visitorType, pageSign);

			//对象赋值
			stats.setPv(kpi_list.get(0));
			stats.setUv(kpi_list.get(1));
			stats.setIpCount(kpi_list.get(2));
			stats.setTotalVisitPage(kpi_list.get(3));
			stats.setJumpCount(kpi_list.get(4));
			stats.setTotalVisitTime(kpi_list.get(5));

			// 存储在hbase中
			dao.putObject(rowkey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("error to WebSiteEntryPageStatsStorage", e);
		}
		
		return 0;
	}
}
