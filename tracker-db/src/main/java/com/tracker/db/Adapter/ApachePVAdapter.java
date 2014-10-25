package com.tracker.db.Adapter;

import java.io.IOException;
import java.lang.reflect.Field;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.tracker.db.table.ApachePVDesc;
/**
 * 
 * 文件名：ApachePVAdapter
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:14:54
 * 功能描述：废弃
 *
 */
public class ApachePVAdapter extends Adapter{
	//not use
	
	private String webId ;
	private String city;
	private long visitTime;
	private String curUrl;
	private String ip;
	private String os;
	private String browser;
	private boolean isCookieEnable;
	private String language;
	
	public String getWebId() {
		return webId;
	}
	public void setWebId(String webId) {
		this.webId = webId;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public long getVisitTime() {
		return visitTime;
	}
	public void setVisitTime(long visitTime) {
		this.visitTime = visitTime;
	}
	public String getCurUrl() {
		return curUrl;
	}
	public void setCurUrl(String curUrl) {
		this.curUrl = curUrl;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getOs() {
		return os;
	}
	public void setOs(String os) {
		this.os = os;
	}
	public String getBrowser() {
		return browser;
	}
	public void setBrowser(String browser) {
		this.browser = browser;
	}
	public boolean isCookieEnable() {
		return isCookieEnable;
	}
	public void setCookieEnable(boolean isCookieEnable) {
		this.isCookieEnable = isCookieEnable;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf,"table".getBytes());
		ResultScanner results = table.getScanner(new Scan());
		Result res = null;
		AdapterImpl<ApachePVAdapter, ApachePVDesc> adapterImpl = new AdapterImpl<ApachePVAdapter, ApachePVDesc>(
				ApachePVAdapter.class,new ApachePVDesc());
		ApachePVAdapter tmp = null;
		while((res = results.next()) != null){
			tmp = adapterImpl.Adapters(res);
			System.out.println(tmp.getBrowser());
			System.out.println(tmp.getOs());
			System.out.println(tmp.getWebId());
		}

	}
}
