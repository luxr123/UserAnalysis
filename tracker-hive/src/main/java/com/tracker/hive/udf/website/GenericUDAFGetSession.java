package com.tracker.hive.udf.website;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.constant.website.SysEnvType;
import com.tracker.hive.service.SessionService;

/**
 * 按照各个维度用户分组,并解析用户所有会话信息
 * @author xiaorui.lu
 * 
 */
public class GenericUDAFGetSession extends AbstractGenericUDAFResolver {
	private static final Logger LOG = LoggerFactory.getLogger(GenericUDAFGetSession.class);

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		//指定调用的Evaluator,用来接收消息和指定UDAF如何调用
		return new GenericUDAFGetSessionEvaluator();
	}

	public static class GenericUDAFGetSessionEvaluator extends GenericUDAFEvaluator {
		private Long ckct;
		private String pageId;
		private String refDomain;
		private String refKeyword;
		private String refSubDomain;
		private Integer refType;
		private Long visitTime;

		private String brower;
		private String os;
		private String colorDepth;
		private String cookieEnabled;
		private String language;
		private String screen;

		private Map<SysEnvType, String> sysEnvMap;
		
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			//指定返回值类型
			return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			//字段赋值
			ckct = Long.parseLong(parameters[0].toString());
			pageId = parameters[1].toString();
			visitTime = Long.parseLong(parameters[2].toString());
			refType = Integer.parseInt(parameters[3].toString());
			refKeyword = parameters[4].toString();
			refDomain = parameters[5].toString();
			refSubDomain = parameters[6].toString();

			brower = parameters[7].toString();
			os = parameters[8].toString();
			colorDepth = parameters[9].toString();
			cookieEnabled = parameters[10].toString();
			language = parameters[11].toString();
			screen = parameters[12].toString();

			sysEnvMap = new HashMap<SysEnvType, String>();
			sysEnvMap.put(SysEnvType.BROWSER, brower);
			sysEnvMap.put(SysEnvType.OS, os);
			sysEnvMap.put(SysEnvType.COLOR_DEPTH, colorDepth);
			sysEnvMap.put(SysEnvType.COOKIE_ENABLED, cookieEnabled);
			sysEnvMap.put(SysEnvType.LANGUAGE, language);
			sysEnvMap.put(SysEnvType.SCREEN, screen);

			SessionAgg sessionAgg = (SessionAgg) agg;
			try {
				//处理当前记录
				sessionAgg.service.addLog(ckct, pageId, visitTime, refType, refKeyword, refDomain, refSubDomain, sysEnvMap);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			SessionAgg sessionAgg = (SessionAgg) agg;
			sessionAgg.service.endHandle();
			return sessionAgg.service.toString();
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			SessionAgg sessionAgg = (SessionAgg) agg;
			sessionAgg.result += partial.toString();
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			SessionAgg sessionAgg = (SessionAgg) agg;
			return sessionAgg.result;
		}
		
		
		
		//获取聚集buffer
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			SessionAgg result = new SessionAgg();
			reset(result);
			return result;
		}

		//重置buffer
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((SessionAgg) agg).service = new SessionService();
			((SessionAgg) agg).result = "";
		}
		
		static class SessionAgg implements AggregationBuffer {
			SessionService service;
			String result;
		}
	}
}
