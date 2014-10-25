package com.tracker.data.table.searchData;

/**
 * 搜索经理人条件类型
 */
public enum ManagerSearchConType {
	DIV(1, "公司部门", "div"), //
	CUR_COMPANY(2, "公司", "company"), //
	NISCOHIS(3, "是否包括过往工作", "niscohis"), //
	AREA(4, "工作地点", "area"), //
	FULLTEXT(5, "全文关键字", "fullText"), //
	NISSENIORDB(6, "人才库", "nisseniordb"), //
	COMPANY_TYPE(7, "公司性质", "companyType"), //
	COMPANY_SIZE(8, "公司规模", "companySize"), //
	CORE_POS(9, "职能", "corePos"), //
	DEGREE(10, "学历", "degree"), // 
	INDUSTRY(11, "行业", "industry"), //
	POS_LEVEL(12, "职能级别", "posLevel"), //
	SEX(13, "性别", "sex"), //
	WORK_YEAR(14, "工作年限", "workYear"),//
	COMPANY_TEXT(15, "公司关键字", "companyText"),
	POST_TEXT(16, "部门职位关键字", "posText");

	private final int type;
	private final String desc;
	private final String field;

	private ManagerSearchConType(int type, String desc, String field) {
		this.type = type;
		this.desc = desc;
		this.field = field;
	}

	public int getType() {
		return type;
	}

	public String getDesc() {
		return desc;
	}

	public String getField() {
		return field;
	}

	public static ManagerSearchConType getType(int id) {
		return null;
	}

}
