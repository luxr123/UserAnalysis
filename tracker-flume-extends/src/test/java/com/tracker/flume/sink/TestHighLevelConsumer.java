package com.tracker.flume.sink;

import java.util.List;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;

import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.utils.JsonUtil;
import com.tracker.flume.kafka.partitioner.IntegerDecoder;

public class TestHighLevelConsumer {
	public static void main(String[] args) throws Exception {
			String zkConnect = "10.100.2.92,10.100.2.93";
			String groupId = "apacheLog2";
			String topic = "apachePVLog";
//			ZkUtils.maybeDeletePath(zkConnect, "/consumers/" + groupId);
			
			Properties props = new Properties();
			props.put("group.id", groupId);
			props.put("zookeeper.connect", zkConnect);
//			props.put("auto.offset.reset", "smallest");
			props.put("consumer.id", "TestHighLevelConsumer");
//			props.put("key.serializer.class", IntegerEncoder.class.getName()); // key serializer
//			props.put("serializer.class", "kafka.serializer.StringEncoder"); // value serializer
			
			ConsumerConfig consumerConfig = new ConsumerConfig(props);
			ConsumerConnector consumer = kafka.consumer.Consumer
					.createJavaConsumerConnector(consumerConfig);
			
//			List<KafkaStream<Integer,String>> streams = consumer.createMessageStreamsByFilter(
//					new Whitelist(topic), 1, new IntegerDecoder(null), new StringDecoder(null));
//			ConsumerIterator<Integer,String> it = streams.get(0).iterator();
//			System.out.println(it.next().message());
			
			System.out.println("groupId:" + groupId);
			List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1);
			
//			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//			topicCountMap.put(topic, 1);//tell kafka how many threads we are prodiding for which topics
//			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
			
			ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
			while(true){
				try{
					if(it.nonEmpty()){
						ApacheSearchLog log = JsonUtil.toObject(new String(it.next().message()), ApacheSearchLog.class);
						if(log.getSearchParam() == null)
							System.out.println(JsonUtil.toJson(log));
					}
				} catch(ConsumerTimeoutException e){
				}
			}
		}
}
