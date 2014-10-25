package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 由于hive与hbase集成的时候，只支持String类型，所以存储在hbase的数据类型都为String，取的时候再转到相应的类型上。
 * @author jason
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class Geography {
	public static final int LEVEL_INDEX = 1;
	public static final int ID_INDEX = 2;
	public static final String COUNTRY_OTHER = "其他";
	public static final String PROVINCE_OTHER = "其他";
	public static final String CITY_OTHER = "其他";
	
	@HBaseColumn(qualifier = "countryId", isStoreStringType = true)
	public Integer countryId;
	
	@HBaseColumn(qualifier = "country", isStoreStringType = true)
	public String country;
	
	@HBaseColumn(qualifier = "provinceId", isStoreStringType = true)
	public Integer provinceId;
	
	@HBaseColumn(qualifier = "province", isStoreStringType = true)
	public String province;
	
	@HBaseColumn(qualifier = "city", isStoreStringType = true)
	public String city;
	
	@HBaseColumn(qualifier = "level", isStoreStringType = true)
	public Integer level; //国家级别（1）、省级别（2）、市级别（3）
	
	@HBaseColumn(qualifier = "remark", isStoreStringType = true)
	public String remark;

	public Geography(){}
	
	/**
	 * 生成rowkey值
	 */
	public static String generateRow(int level, int id){
		return generateRowPrefix(level) + id;
	}
	
	public static String generateRowPrefix(int level){
		return  DataKeySign.SIGN_GEOGRAPHY + RowUtil.ROW_SPLIT + level + RowUtil.ROW_SPLIT;
	}
	
	public Integer getCountryId() {
		return countryId;
	}

	public void setCountryId(Integer countryId) {
		this.countryId = countryId;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public Integer getProvinceId() {
		return provinceId;
	}

	public void setProvinceId(Integer provinceId) {
		this.provinceId = provinceId;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Integer getLevel() {
		return level;
	}

	public void setLevel(Integer level) {
		this.level = level;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}

}
