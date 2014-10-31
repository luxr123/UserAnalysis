package com.tracker.db.dao.data.model;

import com.tracker.common.utils.JsonUtil;
import com.tracker.db.dao.data.DataKeySign;
import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.util.RowUtil;

/**
 * 文件名：Geography
 * 创建人：jason.hua
 * 创建日期：2014-10-27 上午11:08:48
 * 功能描述：地域数据字典
 *
 */
@HBaseTable(tableName = "d_dictionary", defaultFamily = "data")
public class Geography {
	/**
	 * row中各个字段index值
	 */
	public static final int LEVEL_INDEX = 1;
	public static final int ID_INDEX = 2;
	
	/**
	 * 默认常量值
	 */
	public static final String COUNTRY_OTHER = "其他";
	public static final String PROVINCE_OTHER = "其他";
	public static final String CITY_OTHER = "其他";
	
	@HBaseColumn(qualifier = "countryId", isStoreStringType = true)
	public Integer countryId; //国家id
	
	@HBaseColumn(qualifier = "country", isStoreStringType = true)
	public String country; //国家名
	
	@HBaseColumn(qualifier = "provinceId", isStoreStringType = true)
	public Integer provinceId; //省id
	
	@HBaseColumn(qualifier = "province", isStoreStringType = true)
	public String province; //省名
	
	@HBaseColumn(qualifier = "city", isStoreStringType = true)
	public String city; //城市名
	
	@HBaseColumn(qualifier = "level", isStoreStringType = true)
	public Integer level; //国家级别（1）、省级别（2）、市级别（3）
	
	@HBaseColumn(qualifier = "remark", isStoreStringType = true)
	public String remark; //级别中文名

	public Geography(){}
	
	/**
	 * 函数名：generateRow
	 * 功能描述：生成rowkey值
	 * @param level 级别， 国家（1）， 省（2）， 市（3）
	 * @param id 国家id
	 * @return
	 */
	public static String generateRow(int level, int id){
		return generateRowPrefix(level) + id;
	}
	
	/**
	 * 
	 * 函数名：generateRowPrefix
	 * 功能描述：生成row前缀
	 * @param level 级别， 国家（1）， 省（2）， 市（3）
	 * @return
	 */
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
