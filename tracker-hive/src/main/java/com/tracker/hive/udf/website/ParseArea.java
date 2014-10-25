package com.tracker.hive.udf.website;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.hive.Constants;
import com.tracker.hive.db.HiveService;

/**
 * 解析访问ip来源位置信息
 * @author xiaorui.lu
 */
public class ParseArea extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(ParseArea.class);
	private static Map<Integer, String> countryProvCache = HiveService.getCountryProvCache();
	private static Map<String, Integer> areaCache = HiveService.getAreaCache();

	private final static int DEFAULT_DATE_ID = -1;
	private final static String OTHER = "其他";

	/**
	 * @param country  国家
	 * @param province 省
	 * @param city     市
	 * @return         主键id
	 */
	public Integer evaluate(Text country, Text province, Text city) {
		Integer areaId = null;
		try {
			String countryStr = (country == null ? OTHER : StringUtils.strip(country.toString()));
			String provinceStr = (province == null ? OTHER : StringUtils.strip(province.toString()));
			String cityStr = (province == null ? OTHER : StringUtils.strip(province.toString()));
			
			//获取areaId
			areaId = areaCache.get(countryStr + provinceStr + cityStr);
			if(areaId == null){
				areaId = areaCache.get(countryStr + provinceStr + OTHER);
			}
			if(areaId == null){
				areaId = areaCache.get(countryStr + OTHER + OTHER);
			}
			if(areaId == null){
				areaId = areaCache.get(OTHER + OTHER + OTHER);
			}
		} catch (Exception exp) {
			logger.error("parse country:" + country + ",province:" + province + ",city:" + city + " => areaId:" + areaId, exp);
		}
		
		//返回areaId
		return (areaId == null ? DEFAULT_DATE_ID : areaId);
	}

	/**
	 * @param  areaId 唯一id号
	 * @param  pos 值为:0->country, 1->province
	 * @return country_id or province_id
	 */
	public Integer evaluate(int areaId, int pos) {
		try {
			String flag = countryProvCache.get(areaId);
			if (flag != null) {
				return Integer.parseInt(flag.split(Constants.SPLIT)[pos]);
			}
		} catch (Exception exp) {
			logger.error("error to get countryId or provinceId, areaId:" + areaId + ", pos:" + pos, exp);
		}
		
		return DEFAULT_DATE_ID;
	}
}
