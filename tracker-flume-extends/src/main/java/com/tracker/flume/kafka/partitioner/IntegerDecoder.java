package com.tracker.flume.kafka.partitioner;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import com.tracker.common.utils.IntegerUtil;
/**
 * 
 * 文件名：IntegerDecoder
 * 创建人：kris.chen
 * 创建日期：2014-10-27 上午11:08:39
 * 功能描述：Interger转码，从byte[]转Integer类型
 *
 */
public class IntegerDecoder implements Decoder<Integer>{

	public IntegerDecoder(VerifiableProperties prop){
	}
	
	@Override
	public Integer fromBytes(byte[] bytes) {
		return IntegerUtil.toInt(bytes);
	}

}
