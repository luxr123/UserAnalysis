package com.tracker.hive.udf.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * 字段分发多行
 * @author xiaorui.lu
 * 
 */
public class GenericUDTFExplode extends GenericUDTF {
	private transient ObjectInspector inputOI = null;
	private transient ObjectInspector inputOIKey = null;
	private transient ObjectInspector inputOIValue = null;
	private transient final Object[] forwardListObj = new Object[1];
	private transient final Object[] forwardMapObj = new Object[2];
	
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentException("explode() takes only one argument");
		}
		
		//设置返回类型
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		switch (args[0].getCategory()) {
		case LIST:
			inputOI = args[0];
			
			fieldNames.add("col");
			
			fieldOIs.add(((ListObjectInspector) inputOI).getListElementObjectInspector());
			break;
		case MAP:
			inputOI = args[0];
			
			fieldNames.add("key");
			fieldNames.add("value");
			
			inputOIKey = ((MapObjectInspector) inputOI).getMapKeyObjectInspector();
			inputOIValue = ((MapObjectInspector) inputOI).getMapValueObjectInspector();
			
			fieldOIs.add(inputOIKey);
			fieldOIs.add(inputOIValue);
			break;
		default:
			throw new UDFArgumentException("explode() takes an array or a map as a parameter");
		}

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] o) throws HiveException {
		switch (inputOI.getCategory()) {
			case LIST:
				ListObjectInspector listOI = (ListObjectInspector) inputOI;
				List<?> list = listOI.getList(o[0]);
				
				if (list == null) {
					return;
				}
				
				for (Object r : list) {
					forwardListObj[0] = r;
					//返回结果
					forward(forwardListObj);
				}
				break;
			case MAP:
				MapObjectInspector mapOI = (MapObjectInspector) inputOI;
				Map<?, ?> map = mapOI.getMap(o[0]);
				
				if (map == null) {
					return;
				}
				
				for (Entry<?, ?> entry : map.entrySet()) {
					Object value = entry.getValue();
					if (value == null) {
						continue;
					}
						
					Object key = ObjectInspectorUtils.copyToStandardObject(entry.getKey(), inputOIKey);
					for (String str : value.toString().split(",")) {
						forwardMapObj[0] = key;
						forwardMapObj[1] = ObjectInspectorUtils.copyToStandardObject(new Text(str), inputOIValue);
						//返回对象
						forward(forwardMapObj);
					}
				}
				break;
			default:
				throw new TaskExecutionException("explode() can only operate on an array or a map");
		}
	}

	@Override
	public String toString() {
		return "explode";
	}
	
	@Override
	public void close() throws HiveException {
		
	}
}
