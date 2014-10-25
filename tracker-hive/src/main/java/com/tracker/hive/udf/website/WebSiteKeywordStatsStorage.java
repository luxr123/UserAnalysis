package com.tracker.hive.udf.website;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.webstats.model.WebSiteKeywordStats;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.hive.Constants;
import com.tracker.hive.udf.UDFUtils;

/**
 * 存储搜索关键词的统计信息到hbase中
 * @author xiaorui.lu
 * 
 */
public class WebSiteKeywordStatsStorage extends UDF {
	private static Logger logger = LoggerFactory.getLogger(WebSiteKeywordStatsStorage.class);
	private HBaseDao dao = new HBaseDao(HbaseUtils.getHConnection(Constants.ZOOKEEPERHOST), WebSiteKeywordStats.class);
	private WebSiteKeywordStats stats = new WebSiteKeywordStats();

	private String visitorType;
	private String domainId;

	public int evaluate(Integer dateType, Integer webId, String date, List<Long> list, Map<String, String> map) {
		try {
			visitorType = map.get("vistor_type");
			domainId = map.get("domainId");
			
			Integer visitTypeId = (visitorType == null ? null : Integer.parseInt(visitorType));
			String rowRefKeyword = map.get("ref_keyword");

			//构造rowkey
			String row = WebSiteKeywordStats.generateRow(webId, UDFUtils.getDateType(dateType), date, visitTypeId, domainId, rowRefKeyword);
			
			//对象赋值
			stats.setUv(list.get(0));
			stats.setIpCount(list.get(1));
			stats.setPv(list.get(2));
			stats.setVisitTimes(list.get(3));
			stats.setTotalVisitTime(list.get(4));
			stats.setJumpCount(list.get(5));
			
			//存储对象
			dao.putObject(row, stats);
			
			return 1;
		} catch (NumberFormatException e) {
			logger.error("WebsiteKeywordStatsStorage", e);
		} catch (Exception e) {
			logger.error("WebsiteKeywordStatsStorage", e);
		}
		
		return 0;
	}
}
