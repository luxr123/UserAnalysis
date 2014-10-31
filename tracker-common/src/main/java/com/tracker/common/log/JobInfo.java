package com.tracker.common.log;

/**
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年10月27日 下午4:54:11]
 * @Version: [v1.0]
 * 
 */
public class JobInfo {

	public static Integer CATEGORY = 1;
	public static Integer PROPERTY = 2;
	public static Integer WORKPLACE = 3;
	public static Integer WORKYEAR = 4;
	public static Integer PROFESSIONAL = 5;
	public static Integer SALARY = 6;
	public static Integer EDUCATION = 7;
	public static Integer AGE = 8;
	public static Integer GENDER = 9;

	private String jobId;
	private int category;
	private int job_property;
	private int workplace;
	private int workyear;
	private int professional;
	private int salary;
	private int education;
	private int age;
	private int gender;
	private String beginDate;
	private String endDate;

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public int getCategory() {
		return category;
	}

	public void setCategory(int category) {
		this.category = category;
	}

	public int getJob_property() {
		return job_property;
	}

	public void setJob_property(int job_property) {
		this.job_property = job_property;
	}

	public int getWorkplace() {
		return workplace;
	}

	public void setWorkplace(int workplace) {
		this.workplace = workplace;
	}

	public int getWorkyear() {
		return workyear;
	}

	public void setWorkyear(int workyear) {
		this.workyear = workyear;
	}

	public int getProfessional() {
		return professional;
	}

	public void setProfessional(int professional) {
		this.professional = professional;
	}

	public int getSalary() {
		return salary;
	}

	public void setSalary(int salary) {
		this.salary = salary;
	}

	public int getEducation() {
		return education;
	}

	public void setEducation(int education) {
		this.education = education;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public int getGender() {
		return gender;
	}

	public void setGender(int gender) {
		this.gender = gender;
	}

	public String getBeginDate() {
		return beginDate;
	}

	public void setBeginDate(String beginDate) {
		this.beginDate = beginDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

}
