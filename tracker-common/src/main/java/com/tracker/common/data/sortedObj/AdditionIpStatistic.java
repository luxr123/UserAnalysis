package com.tracker.common.data.sortedObj;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.DecimalFormat;

public class AdditionIpStatistic extends AdditionStored<String> implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -807261362430763205L;
	private String m_ip;
	private int m_count;
	public long m_lastVisitTime;
	public long m_firstVisitTime;
	public String m_webEngine;
	
	public AdditionIpStatistic(String ip,int count,long first,long last,String webEngine){
		m_ip = ip;
		m_count = count;
		m_firstVisitTime = first;
		m_lastVisitTime = last;
		m_webEngine = webEngine;
	}
	
	public AdditionIpStatistic(String ip,String webEngine){
		this(ip,System.currentTimeMillis(),webEngine);
	}
	
	public AdditionIpStatistic(String ip,Long visitTime,String webEngine){
		this(ip,1,visitTime,visitTime,webEngine);
	}
	
	@Override
	public String getkey() {
		// TODO Auto-generated method stub
		return m_ip;
	}

	@Override
	public void merge(AdditionStored val) {
		// TODO Auto-generated method stub
		AdditionIpStatistic value = (AdditionIpStatistic)val;
		m_count++;
		if(m_lastVisitTime < value.m_lastVisitTime)
			m_lastVisitTime = value.m_lastVisitTime;
		if(m_firstVisitTime > value.m_firstVisitTime)
			m_firstVisitTime = value.m_firstVisitTime;
	}
	
	public static AdditionIpStatistic  parseFrom(String origin){
		String splits [] = {};
		if(origin == null && (splits = origin.split(":")).length != 4)
			return null;
		splits = origin.split(":");
		return new AdditionIpStatistic(splits[0],Integer.parseInt(splits[2]),Long.parseLong(splits[3]),Long.parseLong(splits[1]),splits[4]);
	}
	
	public String toString(){
		return m_ip + ":" + m_lastVisitTime + ":" + m_count + ":" + m_firstVisitTime + ":" + m_webEngine;
	}

	public String getIP() {
		return m_ip;
	}
	
	public String getWebEngine(){
		return m_webEngine;
	}

	public Integer getCount() {
		return m_count;
	}

	public Long getVistiTime() {
		return m_lastVisitTime;
	}

	public Long getStartTime() {
		return m_firstVisitTime;
	}
}
