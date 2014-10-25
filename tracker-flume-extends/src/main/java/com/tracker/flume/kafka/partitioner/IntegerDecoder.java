package com.tracker.flume.kafka.partitioner;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import com.tracker.common.utils.IntegerUtil;

public class IntegerDecoder implements Decoder<Integer>{

	public IntegerDecoder(VerifiableProperties prop){
	}
	
	@Override
	public Integer fromBytes(byte[] bytes) {
		return IntegerUtil.toInt(bytes);
	}

}
