package com.tracker.db.dao.data.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;
import com.tracker.db.util.Util;

/**
 * 文件名：SiteSearchDefaultValue
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:46:26
 * 功能描述：搜索默认条件值数据字段
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchDefaultValue {
	/**
	 * row中各个字段index值
	 */
	public static final int SE_ID_INDEX = 1;
	public static final int SEARCH_SHOW_TYPE_INDEX = 2;
	public static final int SEARCH_CONDITION_TYPE_INDEX = 3;
			
	@HBaseColumn(qualifier = "ch_name")
	public String ch_name; //搜索条件中文名
	
	@HBaseColumn(qualifier = "field")
	public String field; //搜索条件字段名（类中）
	
	@HBaseColumn(qualifier = "defaultValue")
	public String defaultValue; //搜索条件默认值
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成rowkey
	 * @param seId 搜索引擎id
	 * @param searchShowType 搜索页面展示类型
	 * @param seConType 搜索条件类型
	 * @return
	 */
	public static String generateRow(Integer seId, Integer searchShowType, Integer seConType){
		Util.checkZeroValue(seConType);
		return generateRowPrefix(seId, searchShowType) + seConType;
	}
	
	/**
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param seId 搜索引擎id
	 * @param searchShowType 搜索页面展示类型
	 * @return
	 */
	public static String generateRowPrefix(Integer seId, Integer searchShowType){
		Util.checkZeroValue(seId);
		Util.checkZeroValue(searchShowType);
		return DataKeySign.SIGN_SEARCH_DEFAULT_VALUE  + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT + (searchShowType == null?"":searchShowType) + RowUtil.ROW_SPLIT;
	}
	
	public String getCh_name() {
		return ch_name;
	}

	public void setCh_name(String ch_name) {
		this.ch_name = ch_name;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public static void main(String[] args) {
		HBaseDao siteSEDao = new HBaseDao(HbaseUtils.getHConnection("10.100.2.92"), SiteSearchDefaultValue.class);

		Map<String, String> result = new HashMap<String, String>();
		
		Integer siteSeId = 2;
		Integer searchShowType = 2;
		
		List<SiteSearchDefaultValue> defaultValueList = siteSEDao.findObjectListByRowPrefix(SiteSearchDefaultValue.generateRowPrefix(siteSeId, searchShowType), SiteSearchDefaultValue.class, null);
		for(SiteSearchDefaultValue defaultValue: defaultValueList){
			result.put(defaultValue.getField(), defaultValue.getDefaultValue());
		}
		System.out.println(result);
	}
}
