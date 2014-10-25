package com.tracker.hive.udf.website;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSitePageStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储访问页面的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
@Description(name = "websitePageStats", value = "_FUNC_(array<bigint>) - insert into hbase")
public class WebSitePageStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSitePageStatsStorage.class);
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSitePageStats.class);
	private WebSitePageStats stats = new WebSitePageStats();

	public int evaluate(Integer dateType, Integer webId, String dt, Integer visitorType, String pageSign, List<Long> kpi_list) {
		try {
			//构造rowkey
			String rowkey = WebSitePageStats.generateRow(webId, UDFUtils.getDateType(dateType), dt, visitorType, pageSign);
			
			//对象赋值
			stats.setPv(kpi_list.get(0));
			stats.setUv(kpi_list.get(1));
			stats.setIpCount(kpi_list.get(2));
			stats.setEntryPageCount(kpi_list.get(3));
			stats.setNextPageCount(kpi_list.get(4));
			stats.setTotalStayTime(kpi_list.get(5));
			stats.setOutPageCount(kpi_list.get(6));

			// 存储在hbase中
			dao.putObject(rowkey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("WebSitePageStatsStorage", e);
			
			return 0;
		}
	}
}
