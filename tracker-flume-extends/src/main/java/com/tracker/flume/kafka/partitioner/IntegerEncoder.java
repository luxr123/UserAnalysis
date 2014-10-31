package com.tracker.flume.kafka.partitioner;

import kafka.utils.VerifiableProperties;

import com.tracker.common.utils.IntegerUtil;

/**
 * 
 * 文件名：IntegerEncoder
 * 创建人：kris.chen
 * 创建日期：2014-10-27 上午11:10:26
 * 功能描述：Integer转byte[]
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
