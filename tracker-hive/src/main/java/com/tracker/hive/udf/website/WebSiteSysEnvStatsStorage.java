package com.tracker.hive.udf.website;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteSysEnvStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储系统环境的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
@Description(name = "websiteSysEnvStats", value = "_FUNC_(array<bigint>) - insert into hbase")
public class WebSiteSysEnvStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSiteSysEnvStatsStorage.class);
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteSysEnvStats.class);
	private WebSiteSysEnvStats stats = new WebSiteSysEnvStats();


	public int evaluate(Integer dateType, Integer webId, String dt, Integer visitorType, Integer sysType, String name,
			List<Long> kpi_list) {
		try {
			//生成rowkey
			String rowKey = WebSiteSysEnvStats.generateRow(webId, UDFUtils.getDateType(dateType), dt, visitorType, sysType, name);

			//对象赋值
			stats.setPv(kpi_list.get(0));
			stats.setUv(kpi_list.get(1));
			stats.setIpCount(kpi_list.get(2));
			stats.setVisitTimes(kpi_list.get(3));
			stats.setTotalVisitTime(kpi_list.get(4));
			stats.setJumpCount(kpi_list.get(5));

			// 存储在hbase中
			dao.putObject(rowKey, stats);
			
			return 1;
		} catch (Exception e) {
			logger.error("WebsiteSysEnvStatsStorage", e);
			
			return 0;
		}
	}
}
