package com.tracker.flume.kafka.partitioner;

import com.tracker.common.utils.IntegerUtil;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 *  props.put("partitioner.class", "com.tracer.test.kafka.custom.ProducerPartitioner")
 * @author jason.hua
 *
 */
public class ProducerPartitioner implements Partitioner<byte[]>{
	
	public ProducerPartitioner(VerifiableProperties prop){
		
	}
	
	@Override
	public int partition(byte[] key, int numPartitions) {
		return IntegerUtil.toInt(key) % numPartitions;
	}
}
