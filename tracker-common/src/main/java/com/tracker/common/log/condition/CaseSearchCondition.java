package com.tracker.common.log.condition;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tracker.common.log.ApacheLog;

/**
 * 搜索经理人条件
 * @author jason.hua
 * 
 */
public class CaseSearchCondition {
	protected static Logger logger = LoggerFactory.getLogger(ApacheLog.class);

	public static enum FIELDS{
		alltext, area, industry, 
		pay, commission, prepaid, exclusive, spyKeywords, caseName
	}
	
	public static Map<String, Field> FIELD_MAP = new HashMap<String, Field>();
	static {
		try {
			FIELD_MAP.put("alltext", CaseSearchCondition.class.getDeclaredField("alltext"));
			FIELD_MAP.put("area", CaseSearchCondition.class.getDeclaredField("area"));
			FIELD_MAP.put("industry", CaseSearchCondition.class.getDeclaredField("industry"));
			FIELD_MAP.put("pay", CaseSearchCondition.class.getDeclaredField("pay"));
			FIELD_MAP.put("commission", CaseSearchCondition.class.getDeclaredField("commission"));
			FIELD_MAP.put("prepaid", CaseSearchCondition.class.getDeclaredField("prepaid"));
			FIELD_MAP.put("exclusive", CaseSearchCondition.class.getDeclaredField("exclusive"));
			FIELD_MAP.put("spyKeywords", CaseSearchCondition.class.getDeclaredField("spyKeywords"));
			FIELD_MAP.put("caseName", CaseSearchCondition.class.getDeclaredField("caseName"));
		} catch (NoSuchFieldException e) {
			logger.error("CaseSearchCondition Field", e);
		} catch (SecurityException e) {
			logger.error("CaseSearchCondition Field", e);
		}
	}
	
	public static final String SEARCH_ENGIN_NAME = "CaseEngine";

	// 搜索条件
	public String alltext; // 是否全文搜索
	public String area; // 工作地点
	public String industry; // 行业列表
	public String pay; // 年薪范围
	public String commission; // 佣金范围
	public String prepaid; // 是否有项目启动金
	public String exclusive; // 是否独家
	public String spyKeywords; // 猎头关键词
	public String caseName; // case关键词

	public String getAlltext() {
		return alltext;
	}

	public void setAlltext(String alltext) {
		this.alltext = alltext;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getIndustry() {
		return industry;
	}

	public void setIndustry(String industry) {
		this.industry = industry;
	}

	public String getPay() {
		return pay;
	}

	public void setPay(String pay) {
		this.pay = pay;
	}

	public String getCommission() {
		return commission;
	}

	public void setCommission(String commission) {
		this.commission = commission;
	}

	public String getPrepaid() {
		return prepaid;
	}

	public void setPrepaid(String prepaid) {
		this.prepaid = prepaid;
	}

	public String getExclusive() {
		return exclusive;
	}

	public void setExclusive(String exclusive) {
		this.exclusive = exclusive;
	}

	public String getSpyKeywords() {
		return spyKeywords;
	}

	public void setSpyKeywords(String spyKeywords) {
		this.spyKeywords = spyKeywords;
	}

	public String getCaseName() {
		return caseName;
	}

	public void setCaseName(String caseName) {
		this.caseName = caseName;
	}

	public static void main(String[] args) {
//		String json = "{'webId':'1','category':'FoxEngine', 'searchType':'1', 'fullText':'text'}";
//		ApacheSearchLog log = JsonUtil.toObject(json, ApacheSearchLog.class);
//		System.out.println(log.getWebId());
//		System.out.println(log.getCategory());
//		
//		if(log.getCategory().equalsIgnoreCase("foxengine")){
//			ManagerSearchLog searchLog = JsonUtil.toObject(json, ManagerSearchLog.class);
//			System.out.println(searchLog.fullText);
//			System.out.println(searchLog.searchType);
//		}
		
		
	}	
}