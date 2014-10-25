package com.tracker.hive.udf.website;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.tracker.common.constant.website.SysEnvType;

/**
 * 解析系统环境并分发成多行
 * @author xiaorui.lu
 * 
 */
public class GenericUDTFParseEnv extends GenericUDTF {
	private SysEnvType[] envTypes = { SysEnvType.BROWSER, SysEnvType.OS, SysEnvType.COLOR_DEPTH, SysEnvType.COOKIE_ENABLED, SysEnvType.LANGUAGE, SysEnvType.SCREEN };
	private Object[] result = new Object[2];
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException(GenericUDTFParseEnv.class.getName() + " takes string as a parameter");
		}
		
		//设置返回值类型
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		
		fieldNames.add("systemType");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		
		fieldNames.add("systemName");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		// browser ,os ,color_depth ,cookie_enabled ,language ,screen
		for (int i = 0; i < args.length; i++) {
			result[0] = envTypes[i].getValue();
			result[1] = args[i].toString();
			
			//返回处理结果
			forward(result);
		}
	}

	@Override
	public void close() throws HiveException {
		
	}
}
