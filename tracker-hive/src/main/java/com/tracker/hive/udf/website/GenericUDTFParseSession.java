package com.tracker.hive.udf.website;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.constant.website.SysEnvType;

/**
 * 解析用户所有会话信息,并分发成多行数据
 * @author xiaorui.lu
 */
public class GenericUDTFParseSession extends GenericUDTF {
	private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFParseSession.class);
	private Object[] result = new Object[28];
	
	private JSONArray sessioArray;
    private JSONObject sessionObject;
	private JSONObject sysEnvObject;
	private JSONArray pageArray;

	private Long sessionJumpCount;
	private String sessionPageId;
	private Integer refType;
	private String domain;
	private String refKeyword;
	private Integer serverDateId;
	private Integer serverTimeId;
	private Long sessionTime;
	private Long totalPage;
	private Long visitTimes;
	private Integer visitorTypeOfDay;
	private Integer visitorTypeOfWeek;
	private Integer visitorTypeOfMonth;
	private Integer visitorTypeOfYear;

	private String browser;
	private String os;
	private String color_depth;
	private String cookie_enabled;
	private String language;
	private String screen;

	// page
	private Integer entryPageCount;
	private Integer pageJumpCount;
	private Integer nextPageCount;
	private String nextPageId;
	private Integer outPageCount;
	private String pageId;
	private Integer pv;
	private Integer stayTime;

	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException(GenericUDTFParseSession.class.getName() + " takes string as a parameter");
		}
		
		//设置返回类型
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		
		fieldNames.add("serverDateId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("serverTimeId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("refType");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("refKeyword");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("sessionPageId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("visitorTypeOfDay");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("visitorTypeOfWeek");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("visitorTypeOfMonth");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("visitorTypeOfYear");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("sessionTime");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldNames.add("totalPage");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldNames.add("visitTimes");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldNames.add("sessionJumpCount");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldNames.add("pageId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("nextPageId");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("pv");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("entryPageCount");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("nextPageCount");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("outPageCount");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("jumpCount");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("stayTime");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldNames.add("browser");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("os");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("color_depth");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("cookie_enabled");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("language");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("screen");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("domain");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}


	@Override
	public void process(Object[] args) throws HiveException {
		try {
			sessioArray = new JSONArray(args[0].toString());
			
			for (int i = 0; i < sessioArray.length(); i++) {
				sessionObject = sessioArray.optJSONObject(i);
				
				sessionJumpCount = sessionObject.optLong("jumpCount");
				sessionPageId = sessionObject.optString("pageSign");
				refType = sessionObject.optInt("refType");
				domain = sessionObject.optString("domain");
				refKeyword = sessionObject.optString("refKeyword");
				serverDateId = sessionObject.optInt("serverDateId");
				serverTimeId = sessionObject.optInt("serverTimeId");
				sessionTime = sessionObject.optLong("sessionTime");
				totalPage = sessionObject.optLong("totalPage");
				visitTimes = sessionObject.optLong("visitTimes");
				visitorTypeOfDay = sessionObject.optInt("visitorTypeOfDay");
				visitorTypeOfWeek = sessionObject.optInt("visitorTypeOfWeek");
				visitorTypeOfMonth = sessionObject.optInt("visitorTypeOfMonth");
				visitorTypeOfYear = sessionObject.optInt("visitorTypeOfYear");

				// sysEnv
				sysEnvObject = sessionObject.optJSONObject("sysEnvEntity").optJSONObject("result");
				
				browser = sysEnvObject.optString(SysEnvType.BROWSER.toString());
				os = sysEnvObject.optString(SysEnvType.OS.toString());
				color_depth = sysEnvObject.optString(SysEnvType.COLOR_DEPTH.toString());
				cookie_enabled = sysEnvObject.optString(SysEnvType.COOKIE_ENABLED.toString());
				language = sysEnvObject.optString(SysEnvType.LANGUAGE.toString());
				screen = sysEnvObject.optString(SysEnvType.SCREEN.toString());

				pageArray = sessionObject.optJSONArray("pageEntities");
				
				for (int j = 0; j < pageArray.length(); j++) {
					JSONObject pageObject = pageArray.optJSONObject(j);
					entryPageCount = pageObject.optInt("entryPageCount");
					pageJumpCount = pageObject.optInt("jumpCount");
					nextPageCount = pageObject.optInt("nextPageCount");
					nextPageId = pageObject.optString("nextPageSign");
					outPageCount = pageObject.optInt("outPageCount");
					pageId = pageObject.optString("pageSign");
					pv = pageObject.optInt("pv");
					stayTime = pageObject.optInt("stayTime");

					//对象赋值
					result[0] = serverDateId;
					result[1] = serverTimeId;
					result[2] = refType;
					result[3] = refKeyword;
					result[4] = sessionPageId;
					result[5] = visitorTypeOfDay;
					result[6] = visitorTypeOfWeek;
					result[7] = visitorTypeOfMonth;
					result[8] = visitorTypeOfYear;
					result[9] = sessionTime;
					result[10] = totalPage;
					result[11] = visitTimes;
					result[12] = sessionJumpCount;
					result[13] = pageId;
					result[14] = nextPageId;
					result[15] = pv;
					result[16] = entryPageCount;
					result[17] = nextPageCount;
					result[18] = outPageCount;
					result[19] = pageJumpCount;
					result[20] = stayTime;
					result[21] = browser;
					result[22] = os;
					result[23] = color_depth;
					result[24] = cookie_enabled;
					result[25] = language;
					result[26] = screen;
					result[27] = domain;
					
					//返回结果
					forward(result);
				}
			}
		} catch (JSONException e) {
			LOG.error("error to paseSession", e);
		}
	}

	@Override
	public void close() throws HiveException {

	}
}
