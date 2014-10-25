package com.tracker.hive.udf.search;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * 搜索条件次数统计
 * @author xiaorui.lu
 * 
 */
@Description(name = "searchCount", value = "_FUNC_(x) - 返回搜索次数")
public class GenericUDAFSearchConditionCount implements GenericUDAFResolver2 {
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		//指定调用的Evaluator,用来接收消息和指定UDAF如何调用
		return new GenericUDAFCountEvaluator();
	}
	
	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {
		//类型检查
		TypeInfo[] parameters = paramInfo.getParameters();
		if (parameters.length < 3) {
			throw new UDFArgumentException(parameters.length - 1 + " Exactly 3 argument is expected.");
		}
		
		//指定调用的Evaluator,用来接收消息和指定UDAF如何调用
		return new GenericUDAFCountEvaluator();
	}
	

	/**
	 * 文件名：GenericUDAFCountEvaluator
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-22 上午9:37:01
	 * 功能描述：处理逻辑
	 *
	 */
	public static class GenericUDAFCountEvaluator extends GenericUDAFEvaluator {
		private LongObjectInspector partialCountAggOI;
		private LongWritable result;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			partialCountAggOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
			result = new LongWritable(0);
			
			//返回值类型
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			if (parameters == null) {
				return;
			}
			
			//断言检查  如果parameters.length > 0，则程序继续执行，否则程序抛出AssertionError并终止执行
			assert parameters.length > 0;
			
			Integer showType = Integer.parseInt(parameters[0].toString());
			Integer conditionType = Integer.parseInt(parameters[1].toString());
			
			switch (showType) {
				case 1:
					if (conditionType == 3 || conditionType == 6) {
						if (Integer.parseInt(parameters[2].toString()) == 0) {
							break;
						}
					}
					((CountAgg) agg).value++;
					break;
				case 2:
					if (conditionType == 3 || conditionType == 6) {
						if (Integer.parseInt(parameters[2].toString()) == 1) {
							break;
						}
					}
					((CountAgg) agg).value++;
					break;
				default:
					((CountAgg) agg).value++;
					break;
			}
		}

		/**
		 * 合并聚集结果
		 */
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				long p = partialCountAggOI.get(partial);
				
				((CountAgg) agg).value += p;
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			result.set(((CountAgg) agg).value);
			return result;
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return terminate(agg);
		}
		
		
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			CountAgg buffer = new CountAgg();
			reset(buffer);
			
			return buffer;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((CountAgg) agg).value = 0;
		}
		
		/** class for storing count value. */
		@AggregationType(estimable = true)
		static class CountAgg extends AbstractAggregationBuffer {
			long value;

			@Override
			public int estimate() {
				return JavaDataModel.PRIMITIVES2;
			}
		}
	}
}
