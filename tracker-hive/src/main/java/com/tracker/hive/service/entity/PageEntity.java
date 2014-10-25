package com.tracker.hive.service.entity;

/**
 * 页面度量
 * @author jason.hua
 *
 */
public class PageEntity {
	private String pageSign;//页面标识
	private String nextPageSign;//下一页面（的页面标识）
	private long pv = 1; //浏览数
	private long entryPageCount; //入口页次数
	private long nextPageCount;//贡献下游浏览次数
	private long outPageCount;//出口页次数
	private long jumpCount;//跳出次数
	private long stayTime;//总停留时长（second）
	
	
	public PageEntity(){
		
	}
	
	
	public long getPv() {
		return pv;
	}
	public void setPv(long pv) {
		this.pv = pv;
	}

	public long getEntryPageCount() {
		return entryPageCount;
	}

	public void setEntryPageCount(long entryPageCount) {
		this.entryPageCount = entryPageCount;
	}

	public long getNextPageCount() {
		return nextPageCount;
	}

	public void setNextPageCount(long nextPageCount) {
		this.nextPageCount = nextPageCount;
	}

	public long getOutPageCount() {
		return outPageCount;
	}

	public void setOutPageCount(long outPageCount) {
		this.outPageCount = outPageCount;
	}

	public long getJumpCount() {
		return jumpCount;
	}

	public void setJumpCount(long jumpCount) {
		this.jumpCount = jumpCount;
	}

	public long getStayTime() {
		return stayTime;
	}

	public void setStayTime(long stayTime) {
		this.stayTime = stayTime;
	}

	public String getPageSign() {
		return pageSign;
	}

	public void setPageSign(String pageSign) {
		this.pageSign = pageSign;
	}

	public String getNextPageSign() {
		return nextPageSign;
	}

	public void setNextPageSign(String nextPageSign) {
		this.nextPageSign = nextPageSign;
	}
}
