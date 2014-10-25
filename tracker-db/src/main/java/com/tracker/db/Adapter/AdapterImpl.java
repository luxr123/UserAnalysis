package com.tracker.db.Adapter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.table.BaseTableDesc;

public class AdapterImpl<T extends Adapter, E extends BaseTableDesc> {

	public static Object m_cl;
	private E m_tableDesc;

	public AdapterImpl(Class<T> cl, E tableDesc) {
		if (T.m_fields == null) {
			T.m_fields = new ArrayList<Field>();
			T.initFields(tableDesc.getFieldsSize());
			Field[] fields = cl.getDeclaredFields();
			int pos = -1;
			for (Field field : fields) {
				if ((pos = tableDesc.getPos(field.getName())) != -1)
					T.m_fields.set(pos, field);
			}
		}
		m_cl = cl;
		m_tableDesc = tableDesc;
	}

	public T Adapters(Result res) {
		T adapter = null;
		try {
			adapter = (T) ((Class<T>) m_cl).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String field : m_tableDesc.listFields()) {
			byte[] tmp = res.getValue(m_tableDesc.getFieldsColumnFamily(field)
					.getBytes(), field.getBytes());
			// Record dose not has the specidied field
			if (tmp == null)
				continue;
			int pos = m_tableDesc.getPos(field);
			Field fld = T.m_fields.get(pos);
			if (fld == null)
				continue;
			String type = fld.getType().getCanonicalName();
			type = type.substring(type.lastIndexOf('.') + 1, type.length());
			// cast
			String val = T.m_fields.get(pos).getName();
			char[] chars = val.toCharArray();
			chars[0] -= 32;
			val = "set" + new String(chars);
			try {
				if (type.equalsIgnoreCase("integer")
						|| type.equalsIgnoreCase("int")) {
					Method m1 = ((Class<T>) m_cl).getDeclaredMethod(val,
							Integer.class);
					m1.invoke(adapter, Integer.parseInt(Bytes.toString(tmp)));
				} else if (type.equalsIgnoreCase("long")
						|| type.equalsIgnoreCase("Long")) {
					Method m2 = ((Class<T>) m_cl).getDeclaredMethod(val,
							Long.class);
					m2.invoke(adapter, Long.parseLong(Bytes.toString(tmp)));
				} else if (type.equalsIgnoreCase("string")) {
					Method m3 = ((Class<T>) m_cl).getDeclaredMethod(val,
							String.class);
					m3.invoke(adapter, Bytes.toString(tmp));
				} else {
					System.out.println(type + " is unknow");
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		// set rowKey
		adapter.setM_rowKey(Bytes.toString(res.getRow()));
		return adapter;
	}

	public HbaseParam converToHParam(T adapter) {
		HbaseParam param = new HbaseParam();
		for (String field : m_tableDesc.listFields()) {
			int index = m_tableDesc.getPos(field);
			Field fld = T.m_fields.get(index);
			if (fld == null) {
				// Adapter does not have the speciefied field
				continue;
			}
			try {
				String type = fld.getType().getCanonicalName();
				type = type.substring(type.lastIndexOf('.') + 1, type.length());
				String val = null;
				if (type.equalsIgnoreCase("integer")
						|| type.equalsIgnoreCase("int")) {
					val = (new Integer(fld.getInt(adapter))).toString();
				} else if (type.equalsIgnoreCase("long")
						|| type.equalsIgnoreCase("Long")) {
					val = (new Long(fld.getLong(adapter))).toString();
				} else if (type.equalsIgnoreCase("string")) {
					val = fld.get(adapter).toString();
				} else {
					throw new Exception("unknow type");
				}
				String family = m_tableDesc.getFieldsColumnFamily(field);
				param.setValue(family + ":" + field, val);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		param.setRowkey(adapter.getM_rowKey());
		return param;
	}
}
