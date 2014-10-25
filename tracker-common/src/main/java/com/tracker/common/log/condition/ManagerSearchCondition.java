package com.tracker.common.log.condition;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.log.ApacheLog;
import com.tracker.common.utils.FieldHandler;
import com.tracker.common.utils.JsonUtil;

/**
 * 搜索经理人条件
 * @author jason.hua
 * 
 */
public class ManagerSearchCondition {
	protected static Logger logger = LoggerFactory.getLogger(ApacheLog.class);
	private static final String SPLIT = String.valueOf((char) 16); // char(16）用于分隔符

	public static enum FIELDS{
		nisseniordb, niscohis, area, 
		company, div, corePos, posLevel, industry, workYear,
		degree, sex, companyType, companySize, 
		posText, companyText, fullText
	}
	
	public static Map<String, Field> FIELD_MAP = new HashMap<String, Field>();
	static {
		try {
//			FIELD_MAP.put("searchType", ManagerSearchCondition.class.getDeclaredField("searchType"));
			FIELD_MAP.put("area", ManagerSearchCondition.class.getDeclaredField("area"));
			FIELD_MAP.put("companySize", ManagerSearchCondition.class.getDeclaredField("companySize"));
			FIELD_MAP.put("companyText", ManagerSearchCondition.class.getDeclaredField("companyText"));
			FIELD_MAP.put("corePos", ManagerSearchCondition.class.getDeclaredField("corePos"));
			FIELD_MAP.put("companyType", ManagerSearchCondition.class.getDeclaredField("companyType"));
			FIELD_MAP.put("company", ManagerSearchCondition.class.getDeclaredField("company"));
			FIELD_MAP.put("degree", ManagerSearchCondition.class.getDeclaredField("degree"));
			FIELD_MAP.put("fullText", ManagerSearchCondition.class.getDeclaredField("fullText"));
			FIELD_MAP.put("industry", ManagerSearchCondition.class.getDeclaredField("industry"));
			FIELD_MAP.put("posLevel", ManagerSearchCondition.class.getDeclaredField("posLevel"));
			FIELD_MAP.put("posText", ManagerSearchCondition.class.getDeclaredField("posText"));
			FIELD_MAP.put("sex", ManagerSearchCondition.class.getDeclaredField("sex"));
			FIELD_MAP.put("workYear", ManagerSearchCondition.class.getDeclaredField("workYear"));
			FIELD_MAP.put("div", ManagerSearchCondition.class.getDeclaredField("div"));
			FIELD_MAP.put("niscohis", ManagerSearchCondition.class.getDeclaredField("niscohis"));
			FIELD_MAP.put("nisseniordb", ManagerSearchCondition.class.getDeclaredField("nisseniordb"));
		} catch (NoSuchFieldException e) {
			logger.error("ManagerSearchCondition Field", e);
		} catch (SecurityException e) {
			logger.error("ManagerSearchCondition Field", e);
		}
	}
	
	public static final String SEARCH_ENGIN_NAME = "FoxEngine";

	// 搜索条件
//	public Integer searchType; // 搜索引擎类型， 搜索经理人列表（1）、搜索部门统计（3）
	public String nisseniordb; // 人才库，高级人才库（0)、全部人才库（1）
	public String niscohis; // 是否只搜当前工作，不包括过往工作（0）、包括过往工作（1）
	public String area; // 工作地点列表
	public String company; // 公司ID列表
	public String div; // 公司部门ID列表
	public String corePos; // 职能ID列表
	public String posLevel; // 职能级别列表
	public String industry; // 行业列表
	public String workYear; // 工作年限列表
	public String degree; // 学历列表
	public String sex; // 性别列表
	public String companyType; // 公司性质列表
	public String companySize; // 公司规模列表
	public String posText; // 部门职位关键字
	public String companyText; // 公司关键字
	public String fullText; // 全文关键字

	/**
	 * 解析传递给搜索引擎的参数
	 */
	public static ManagerSearchCondition parse(String data) throws UnsupportedEncodingException{
		String[] params = data.split(SPLIT);
		String[] args = new String[17];
		
		int argsNum = 0;
		int paramsNum = 0;
		// 跳过版本类型
		paramsNum++; 
		
		//搜索类型、人才库、过往工作
		while(paramsNum <= 3){
			args[argsNum++] = params[paramsNum++];
		}
		 // 跳过简历更新时间
		paramsNum++;
		
		//数量+值(共12项）
		int listNum = 0;
		while(listNum < 11){
			if(!params[paramsNum++].equals("0")){
				args[argsNum++] = params[paramsNum++];
			} else{
				argsNum++;
			}
			
			listNum++;
		}
		
		 // 跳过起始条数、结束条数
		paramsNum += 2;
		//关键字
		while(paramsNum < params.length){
			args[argsNum++] = params[paramsNum++];
		}
		
		ManagerSearchCondition log = new ManagerSearchCondition();
		log.fillFields(args);
		return log;
	}
	
	public static List<Field> FIELDLIST = new ArrayList<Field>();
	static {
		try {
//			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("searchType"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("nisseniordb"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("niscohis"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("area"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("company"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("div"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("corePos"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("posLevel"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("industry"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("workYear"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("degree"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("sex"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("companyType"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("companySize"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("posText"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("companyText"));
			FIELDLIST.add(ManagerSearchCondition.class.getDeclaredField("fullText"));
		} catch (NoSuchFieldException e) {
			logger.error("ManagerSearchCondition Field", e);
		} catch (SecurityException e) {
			logger.error("ManagerSearchCondition Field", e);
		}
	}
	
	private void fillFields(String [] args){
		for(int i = 0; i< FIELDLIST.size();i++){
			if(null == args[i]){
				continue;
			}
			try {
				if(args[i].trim().length() == 0)
					continue;
				FIELDLIST.get(i).set(this, FieldHandler.stringToObject(FIELDLIST.get(i).getType(), args[i]));
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	public String getNisseniordb() {
		return nisseniordb;
	}

	public void setNisseniordb(String nisseniordb) {
		this.nisseniordb = nisseniordb;
	}

	public String getNiscohis() {
		return niscohis;
	}

	public void setNiscohis(String niscohis) {
		this.niscohis = niscohis;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
	}

	public String getDiv() {
		return div;
	}

	public void setDiv(String div) {
		this.div = div;
	}

	public String getCorePos() {
		return corePos;
	}

	public void setCorePos(String corePos) {
		this.corePos = corePos;
	}

	public String getPosLevel() {
		return posLevel;
	}

	public void setPosLevel(String posLevel) {
		this.posLevel = posLevel;
	}

	public String getIndustry() {
		return industry;
	}

	public void setIndustry(String industry) {
		this.industry = industry;
	}

	public String getWorkYear() {
		return workYear;
	}

	public void setWorkYear(String workYear) {
		this.workYear = workYear;
	}

	public String getDegree() {
		return degree;
	}

	public void setDegree(String degree) {
		this.degree = degree;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getCompanyType() {
		return companyType;
	}

	public void setCompanyType(String companyType) {
		this.companyType = companyType;
	}

	public String getCompanySize() {
		return companySize;
	}

	public void setCompanySize(String companySize) {
		this.companySize = companySize;
	}

	public String getPosText() {
		return posText;
	}

	public void setPosText(String posText) {
		this.posText = posText;
	}

	public String getCompanyText() {
		return companyText;
	}

	public void setCompanyText(String companyText) {
		this.companyText = companyText;
	}

	public String getFullText() {
		return fullText;
	}

	public void setFullText(String fullText) {
		this.fullText = fullText;
	}
	
	public String toString(){
		return JsonUtil.toJson(this);
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
		
		String str = "	11009101000000101190138181910101171100test";
		ManagerSearchCondition log2 = null;
		try {
			log2 = ManagerSearchCondition.parse(str);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		System.out.println(JsonUtil.toJson(log2));
		
	}	
}