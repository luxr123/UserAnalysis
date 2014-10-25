package com.tracker.hive.udf.recsys;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: [xiaorui.lu]
 * @Version: [v1.0]
 * 
 */
public class GenericUDAFGeatherFeature extends AbstractGenericUDAFResolver {

	static final Logger LOG = LoggerFactory.getLogger(GenericUDAFGeatherFeature.class);

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		return new GenericUDAFGeatherFeatureEvaluator();
	}

	/**
	 * <pre>
	 * count_distinct_to_map(array(arg1.arg2)) 
	 * array[] - PARTIAL1 --> arr[Map<key,Map<k,v>>] 
	 * arr[Map<key,Map<k,v>>] - PARTIAL2 --> arr[Map<key,Map<k,v>>] 
	 * arr[Map<key,Map<k,v>>] - FINAL --> Map<key, Map<k,v>> 
	 * array[] - COMPLETE --> Map<key,Map<k,v>>
	 * </pre>
	 */
	public static class GenericUDAFGeatherFeatureEvaluator extends GenericUDAFEvaluator {
		private PrimitiveObjectInspector keyOutputTypeOI;
		private MapObjectInspector valueMapInputTypeOI;
		private PrimitiveObjectInspector valueKeyInputTypeOI;
		private PrimitiveObjectInspector valueValueInputTypeOI;
		private StandardListObjectInspector valueListOutputTypeOI;
		private MapObjectInspector intermediateMapInputTypeOI;
		private ListObjectInspector intermediateListInputTypeOI;
		private StandardMapObjectInspector finalMapTypeOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			/* Setup input OI */
			if (m == Mode.PARTIAL1) {
				valueListOutputTypeOI = (StandardListObjectInspector) parameters[0];
				keyOutputTypeOI = (PrimitiveObjectInspector) valueListOutputTypeOI.getListElementObjectInspector();
				finalMapTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
						PrimitiveObjectInspectorFactory.javaIntObjectInspector);
				return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardMapObjectInspector(
						PrimitiveObjectInspectorFactory.javaIntObjectInspector, finalMapTypeOI));
			} else if (m == Mode.COMPLETE) {
				valueListOutputTypeOI = (StandardListObjectInspector) parameters[0];
				keyOutputTypeOI = (PrimitiveObjectInspector) valueListOutputTypeOI.getListElementObjectInspector();
				finalMapTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
						PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
				return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
						finalMapTypeOI);
			} else if (m == Mode.PARTIAL2) {
				intermediateListInputTypeOI = (ListObjectInspector) parameters[0];
				intermediateMapInputTypeOI = (MapObjectInspector) intermediateListInputTypeOI.getListElementObjectInspector();
				keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) intermediateMapInputTypeOI.getMapKeyObjectInspector();
				valueMapInputTypeOI = (MapObjectInspector) intermediateMapInputTypeOI.getMapValueObjectInspector();
				valueKeyInputTypeOI = (PrimitiveObjectInspector) valueMapInputTypeOI.getMapKeyObjectInspector();
				valueValueInputTypeOI = (PrimitiveObjectInspector) valueMapInputTypeOI.getMapValueObjectInspector();
				finalMapTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(valueKeyInputTypeOI, valueValueInputTypeOI);
				return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardMapObjectInspector(
						keyOutputTypeOI, finalMapTypeOI));
			} else if (m == Mode.FINAL) {
				intermediateListInputTypeOI = (ListObjectInspector) parameters[0];
				intermediateMapInputTypeOI = (MapObjectInspector) intermediateListInputTypeOI.getListElementObjectInspector();
				keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) intermediateMapInputTypeOI.getMapKeyObjectInspector();
				valueMapInputTypeOI = (MapObjectInspector) intermediateMapInputTypeOI.getMapValueObjectInspector();
				valueKeyInputTypeOI = (PrimitiveObjectInspector) valueMapInputTypeOI.getMapKeyObjectInspector();
				valueValueInputTypeOI = (PrimitiveObjectInspector) valueMapInputTypeOI.getMapValueObjectInspector();
				finalMapTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(valueKeyInputTypeOI,
						PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
				return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI, finalMapTypeOI);
			} else {
				throw new IllegalArgumentException("Invalid mode");
			}
		}

		static class FeatureAgg implements AggregationBuffer {
			List<Map<Object, HashMap<Object, Object>>> content;
			Map<Object, HashMap<Object, Object>> featureMap;
			Map<Object, HashMap<Object, Object>> timeLenMap;

			public FeatureAgg() {
				featureMap = new HashMap<Object, HashMap<Object, Object>>();
				timeLenMap = new HashMap<Object, HashMap<Object, Object>>();
				content = new ArrayList<Map<Object, HashMap<Object, Object>>>() {
					{
						add(featureMap);
					}
					{
						add(timeLenMap);
					}
				};
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			FeatureAgg result = new FeatureAgg();
			reset(result);
			return result;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((FeatureAgg) agg).featureMap.clear();
			((FeatureAgg) agg).timeLenMap.clear();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			List<Map<Object, HashMap<Object, Object>>> aggList = ((FeatureAgg) agg).content;
			List<Object> list = (List<Object>) ObjectInspectorUtils.copyToStandardObject(parameters[0], this.valueListOutputTypeOI);

			Map<Object, HashMap<Object, Object>> featureMap = aggList.get(0);
			Map<Object, HashMap<Object, Object>> timeLenMap = aggList.get(1);

			int len = list.size() - 1;// 9
			int timeLen = Integer.valueOf(keyOutputTypeOI.copyObject(list.get(len)).toString());
			for (int i = 0; i < len; i++) {
				Object feature = keyOutputTypeOI.copyObject(list.get(i));
				HashMap<Object, Object> featureVal = featureMap.get(i);
				HashMap<Object, Object> timeLenVal = timeLenMap.get(i);
				if (null == featureVal) {
					featureVal = new HashMap<Object, Object>();
					timeLenVal = new HashMap<Object, Object>();
					featureVal.put(feature, 1);
					timeLenVal.put(feature, timeLen);
				} else {
					Object count = featureVal.get(feature);
					Object lens = timeLenVal.get(feature);
					if (null == count) {
						featureVal.put(feature, 1);
						timeLenVal.put(feature, timeLen);
					} else {
						featureVal.put(feature, Integer.parseInt(count.toString()) + 1);
						timeLenVal.put(feature, Integer.parseInt(lens.toString()) + timeLen);
					}
				}
				featureMap.put(i, featureVal);
				timeLenMap.put(i, timeLenVal);
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return ((FeatureAgg) agg).content;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			List<Map<Object, HashMap<Object, Object>>> aggList = ((FeatureAgg) agg).content;
			Map<Object, HashMap<Object, Object>> featureAgg = aggList.get(0);
			Map<Object, HashMap<Object, Object>> timeLenAgg = aggList.get(1);

			List<Map<Object, HashMap<Object, Object>>> partialResult = (List<Map<Object, HashMap<Object, Object>>>) intermediateListInputTypeOI
					.getList(partial);

			Map<?, ?> featureResult = (Map<?, ?>) ObjectInspectorUtils.copyToStandardObject(partialResult.get(0),
					intermediateMapInputTypeOI);
			for (Entry<?, ?> entry : featureResult.entrySet()) {
				Object type = keyOutputTypeOI.copyObject(entry.getKey());
				Map<?, ?> value = (Map<?, ?>) entry.getValue();

				HashMap<Object, Object> valFeatureAgg = (HashMap<Object, Object>) featureAgg.get(type);

				if (null == valFeatureAgg) {
					valFeatureAgg = new HashMap<Object, Object>();
					valFeatureAgg.putAll(value);
				} else {
					for (Entry<?, ?> ele : value.entrySet()) {
						Object feature = valueKeyInputTypeOI.copyObject(ele.getKey());
						Object count = valueValueInputTypeOI.copyObject(ele.getValue());
						Object sum = valFeatureAgg.get(feature);
						sum = sum != null ? (Integer.parseInt(sum.toString()) + Integer.parseInt(count.toString())) : count;
						valFeatureAgg.put(feature, new IntWritable(Integer.valueOf(sum.toString())));
					}
				}
				featureAgg.put(new IntWritable(Integer.valueOf(type.toString())), valFeatureAgg);
			}

			Map<?, ?> timeLenResult = (Map<?, ?>) ObjectInspectorUtils.copyToStandardObject(partialResult.get(1),
					intermediateMapInputTypeOI);
			for (Entry<?, ?> entry : timeLenResult.entrySet()) {
				Object type = keyOutputTypeOI.copyObject(entry.getKey());
				Map<?, ?> value = (Map<?, ?>) entry.getValue();

				HashMap<Object, Object> valTimeLenAgg = (HashMap<Object, Object>) timeLenAgg.get(type);

				if (null == valTimeLenAgg) {
					valTimeLenAgg = new HashMap<Object, Object>();
					valTimeLenAgg.putAll(value);
				} else {
					for (Entry<?, ?> ele : value.entrySet()) {
						Object feature = valueKeyInputTypeOI.copyObject(ele.getKey());
						Object count = valueValueInputTypeOI.copyObject(ele.getValue());
						Object sum = valTimeLenAgg.get(feature);
						sum = sum != null ? (Integer.parseInt(sum.toString()) + Integer.parseInt(count.toString())) : count;
						valTimeLenAgg.put(feature, new IntWritable(Integer.valueOf(sum.toString())));
					}
				}
				timeLenAgg.put(new IntWritable(Integer.valueOf(type.toString())), valTimeLenAgg);
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			Map<Object, HashMap<Object, Object>> finalMap = new HashMap<Object, HashMap<Object, Object>>();
			Map<Object, HashMap<Object, Object>> tfMulMap = new HashMap<Object, HashMap<Object, Object>>();
			List<Map<Object, HashMap<Object, Object>>> aggList = ((FeatureAgg) agg).content;
			Map<Object, HashMap<Object, Object>> featureAgg = aggList.get(0);
			Map<Object, HashMap<Object, Object>> timeLenAgg = aggList.get(1);

			Map<Object, Object> freqAllSumMap = new HashMap<Object, Object>();
			Map<Object, Object> maxTFMap = new HashMap<Object, Object>();

			for (Entry<Object, HashMap<Object, Object>> entry : featureAgg.entrySet()) {
				int sum = 0;
				int max = Integer.MIN_VALUE;
				Object type = entry.getKey();

				HashMap<Object, Object> timeLenChildMap = timeLenAgg.get(type);
				HashMap<Object, Object> tfChildMap = new HashMap<Object, Object>();
				for (Entry<Object, Object> e : entry.getValue().entrySet()) {
					Object feature = e.getKey();
					Integer count = Integer.valueOf(e.getValue().toString());
					sum += count;

					int tf = count * Integer.valueOf(timeLenChildMap.get(feature).toString());
					max = max < tf ? tf : max;

					tfChildMap.put(feature, tf);
				}
				tfMulMap.put(type, tfChildMap);
				maxTFMap.put(type, max);
				freqAllSumMap.put(type, sum);
			}

			for (Entry<Object, HashMap<Object, Object>> entry : featureAgg.entrySet()) {
				Object type = entry.getKey();
				HashMap<Object, Object> tfChildMap = finalMap.get(type);
				if (tfChildMap == null) {
					tfChildMap = new HashMap<Object, Object>();
					finalMap.put(new IntWritable(Integer.valueOf(type.toString())), tfChildMap);
				}
				for (Entry<Object, Object> e : entry.getValue().entrySet()) {
					Object feature = e.getKey();
					Integer count = Integer.valueOf(e.getValue().toString());

					// calculation
					int tfMul = Integer.valueOf(tfMulMap.get(type).get(feature).toString());
					int tfMax = Integer.valueOf(maxTFMap.get(type).toString());
					int freqSum = Integer.valueOf(freqAllSumMap.get(type).toString());

					double tf = (double) tfMul / tfMax;
					double ipf = Math.log((double) (freqSum + 1) / (freqSum - count + 1));

					tfChildMap.put(feature, tf * ipf);
				}
			}

			return finalMap;
		}
	}
}
