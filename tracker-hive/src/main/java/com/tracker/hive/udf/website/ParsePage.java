package com.tracker.hive.udf.website;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.db.dao.data.model.Page;

/**
 * 解析访问页面信息
 * @author xiaorui.lu
 */
public class ParsePage extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(ParsePage.class);

	public Text evaluate(IntWritable webId, Text pageUrl) {
		try {
			if (webId == null || pageUrl == null) {
				return new Text(Page.OTHER_PAGE_SIGN);
			}

			return new Text(Page.getPageSign(pageUrl.toString()));
		} catch (Exception exp) {
			logger.error("error to ParsePage, webId:" + webId + ", pageUrl:" + pageUrl, exp);
		}
		
		return new Text(Page.OTHER_PAGE_SIGN);
	}
}
