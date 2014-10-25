package com.tracker.hive.udf.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import com.tracker.common.utils.StringUtil;
import com.tracker.hive.db.HiveService;


/**
 * 日期解析类
 * return: [dateId, time]
 * @author xiaorui.lu
 * 
 */
public class GenericUDFParseDateTime extends GenericUDF {
	private transient PrimitiveObjectInspector argumentOI;
	private transient ObjectInspectorConverters.Converter converters;
	private static Map<String, Integer> dateTimeCache = HiveService.getDateTimeCache();
	private static Calendar cal = Calendar.getInstance();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length < 0) {
			throw new UDFArgumentLengthException("GenericUDFParseDateTime requires 1 argument, got " + arguments.length);
		}
		if (arguments[0].getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("GenericUDFParseDateTime only takes primitive types, got " + argumentOI.getTypeName());
		}
		
		//定义类型转换
		argumentOI = (PrimitiveObjectInspector) arguments[0];
		converters = ObjectInspectorConverters.getConverter(argumentOI, PrimitiveObjectInspectorFactory.writableLongObjectInspector);

		//定义输出类型
		List<String> fieldNames = new ArrayList<String>(2) {
			private static final long serialVersionUID = -4000919969473665677L;

			{
				add("date_id");
				add("time");
			}
		};
		
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2) {
			private static final long serialVersionUID = -825558113991893614L;

			{
				add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
				add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
			}
		};

		ObjectInspector outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

		return outputOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		cal.setTimeInMillis(((LongWritable) converters.convert(arguments[0].get())).get());
		
		final String date = cal.get(Calendar.YEAR) + StringUtil.fillLeftData((cal.get(Calendar.MONTH) + 1)) + StringUtil.fillLeftData(cal.get(Calendar.DAY_OF_MONTH));
		//返回日期唯一id，以及小时
		return Arrays.asList(new Object[] { new IntWritable(dateTimeCache.get(date)), new IntWritable(cal.get(Calendar.HOUR_OF_DAY)) });
	}

	@Override
	public String getDisplayString(String[] children) {
		return (children == null ? null : this.getClass().getCanonicalName() + "(" + children[0] + ")");
	}
}
