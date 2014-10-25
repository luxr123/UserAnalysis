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

@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class SiteSearchDefaultValue {
	public static final int SE_ID_INDEX = 1;
	public static final int SEARCH_SHOW_TYPE_INDEX = 2;
	public static final int SEARCH_CONDITION_TYPE_INDEX = 3;
			
	@HBaseColumn(qualifier = "ch_name")
	public String ch_name;
	
	@HBaseColumn(qualifier = "field")
	public String field;
	
	@HBaseColumn(qualifier = "defaultValue")
	public String defaultValue;
	
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

	/**
	 * 生成搜索引擎row
	 */
	public static String generateRow(Integer seId, Integer searchShowType, Integer seConType){
		Util.checkZeroValue(seConType);
		return generateRowPrefix(seId, searchShowType) + seConType;
	}
	
	public static String generateRowPrefix(Integer seId, Integer searchShowType){
		Util.checkZeroValue(seId);
		Util.checkZeroValue(searchShowType);
		return DataKeySign.SIGN_SEARCH_DEFAULT_VALUE  + RowUtil.ROW_SPLIT + seId + RowUtil.ROW_SPLIT + (searchShowType == null?"":searchShowType) + RowUtil.ROW_SPLIT;
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
