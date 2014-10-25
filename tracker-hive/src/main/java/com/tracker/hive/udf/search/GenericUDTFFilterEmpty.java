package com.tracker.hive.udf.search;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * 过滤值为null的字段
 * @author xiaorui.lu
 * 
 */
public class GenericUDTFFilterEmpty extends GenericUDTF {
	private transient ObjectInspector inputOI = null;
	private transient final Object[] forwardMapObj = new Object[1];

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentException("explode() takes only one argument");
		}

		//定义输出类型
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		switch (args[0].getCategory()) {
			case MAP:
				inputOI = args[0];
				fieldNames.add("key");
				fieldOIs.add(((MapObjectInspector) inputOI).getMapKeyObjectInspector());
				break;
			default:
				throw new UDFArgumentException("explode() takes (an array or) a map as a parameter");
		}

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] o) throws HiveException {
		switch (inputOI.getCategory()) {
			case MAP:
				MapObjectInspector mapOI = (MapObjectInspector) inputOI;
				Map<?, ?> map = mapOI.getMap(o[0]);
				
				if (map == null) {
					return;
				}
				
				for (Entry<?, ?> entry : map.entrySet()) {
					if (entry.getValue() == null) {
						continue;
					}
						
					forwardMapObj[0] = entry.getKey();
					//发送对象
					forward(forwardMapObj);
				}
				break;
			default:
				throw new TaskExecutionException("filterEmpty can only operate on an array or a map");
		}
	}

	@Override
	public String toString() {
		return "filter_empty";
	}

	@Override
	public void close() throws HiveException {
		
	}
}
