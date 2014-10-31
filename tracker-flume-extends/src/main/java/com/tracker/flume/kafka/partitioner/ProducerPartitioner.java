package com.tracker.flume.kafka.partitioner;

import com.tracker.common.utils.IntegerUtil;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 
 * 文件名：ProducerPartitioner
 * 创建人：kris.chen
 * 创建日期：2014-10-27 上午11:11:29
 * 功能描述：Producer分区算法，对source过来的数据按partition中的算法发送到相应的分区中
 *
 */
public class ProducerPartitioner implements Partitioner<byte[]>{
	
	public ProducerPartitioner(VerifiableProperties prop){
		
	}
	
	@Override
	/*
	 * 根据key，用分区数取模，对相同的key得到相同的分区号
	 * @see kafka.producer.Partitioner#partition(java.lang.Object, int)
	 */
	public int partition(byte[] key, int numPartitions) {
		return IntegerUtil.toInt(key) % numPartitions;
	}
}
