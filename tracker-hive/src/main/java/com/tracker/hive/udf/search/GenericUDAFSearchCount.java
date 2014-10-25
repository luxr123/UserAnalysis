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
 * 搜索次数统计
 * @author xiaorui.lu
 * 
 */
@Description(name = "searchCount", value = "_FUNC_(x) - 返回搜索次数")
public class GenericUDAFSearchCount implements GenericUDAFResolver2 {
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		return new GenericUDAFCountEvaluator();
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {
		TypeInfo[] parameters = paramInfo.getParameters();

		if (parameters.length < 1) {
			throw new UDFArgumentException(parameters.length - 1 + " Exactly one argument is expected.");
		}
		return new GenericUDAFCountEvaluator();
	}

	/**
	 * GenericUDAFCountEvaluator.
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
			
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			if (parameters == null) {
				return;
			}
				
			assert parameters.length > 0;
			
			boolean isCallSe = Boolean.parseBoolean(parameters[0].toString());
			if (isCallSe) {
				((CountAgg) agg).value++;
			}
		}

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
