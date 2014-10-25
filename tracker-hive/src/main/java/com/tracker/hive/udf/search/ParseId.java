package com.tracker.hive.udf.search;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.hive.db.HiveService;

/**
 * 解析获得Id
 * @author xiaorui.lu
 * 
 */
public class ParseId extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(ParseId.class);

	private static Map<String, Integer> searchEngineMap = HiveService.getSearchEngineCache();

	/**
	 * ParseCategory
	 * @param catagory
	 * @return
	 */
	public Integer evaluate(String catagory) {
		try {
			return searchEngineMap.get(catagory);
		} catch (Exception e) {
			logger.error("error to parseId, catagory:" + catagory, e);
			return -1;
		}
	}

	public static void main(String[] args) {
		System.out.println(searchEngineMap);
	}
}
