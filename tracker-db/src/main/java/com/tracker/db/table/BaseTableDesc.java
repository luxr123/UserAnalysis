package com.tracker.db.table;

import java.util.List;

public interface BaseTableDesc {
	public int getPos(String str);
	public String getFieldsColumnFamily(String field);
	public List<String> listFields();
	public int getFieldsSize();
}
