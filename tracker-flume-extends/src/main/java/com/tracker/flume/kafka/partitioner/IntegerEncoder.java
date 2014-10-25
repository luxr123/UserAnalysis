package com.tracker.flume.kafka.partitioner;

import kafka.utils.VerifiableProperties;

import com.tracker.common.utils.IntegerUtil;

/**
 * props.put("serializer.class", "com.tracer.test.kafka.custom.StringEncoder");
 * @author jason.hua
 *
 */
public class IntegerEncoder implements kafka.serializer.Encoder<Integer> {
	
	public IntegerEncoder(VerifiableProperties prop){
	}
	
	@Override
	public byte[] toBytes(Integer num) {
		if (num != null){
			return IntegerUtil.toBytes(num);
		}
		return null;
	}
}
