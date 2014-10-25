package com.tracker.db.Adapter;

import java.lang.reflect.Field;
import java.util.List;

public abstract class Adapter{
	protected static List<Field> m_fields;
	protected String m_rowKey;
	public String getM_rowKey() {
		return m_rowKey;
	}
	public void setM_rowKey(String m_rowKey) {
		this.m_rowKey = m_rowKey;
	}
	public static void initFields(int size){
		if(m_fields == null) return;
		for(int i = 0;i< size;i++){
			m_fields.add(null);
		}
	}
	public static Adapter createrAdapter(){ return null;};
}
