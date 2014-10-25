package com.tracker.storm.common.spout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * 
 * 文件名：KafkaSpout
 * 创建人：jason.hua
 * 创建日期：2014-10-14 上午11:44:41
 * 功能描述：Spout组件，从kafka中读取数据
 *
 */
public class KafkaSpout implements IRichSpout {
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
	private static final long serialVersionUID = 8836060132127592960L;

	/**
	 * spout组件相关对象
	 */
	public static String STREAM = "kafkaSpoutStream"; //spout发送的streamId名
	private SpoutOutputCollector collector; //用于该spout发送数据
	
	/**
	 * kafka相关对象
	 */
	private final Properties props = new Properties(); //kafka消费者的配置信息
	private final Scheme scheme; //用于解析读取的日志以及申明发送哪些字段
	private final String topic; //指明读取kafka中哪个主题下的数据
	private ConsumerConnector consumer;  //kafka消费者对象
	private ConsumerIterator<byte[], byte[]> stream; //kafka读取数据的流

	/**
	 * 创建KafkaSpout对象
	 * @param zookeeper zookeeper地址，以“，”间隔
	 * @param groupId kafka中consumer的组名
	 * @param topic kafka中topic名
	 * @param scheme 1. 解析从kafka中读取的日志; 2. 获取发送的tuple中，包含哪些字段
	 */
	public KafkaSpout(String zookeeper, String groupId, String topic,
			Scheme scheme) {
		props.put("zookeeper.connect", zookeeper); //zookeeper地址
		props.put("group.id", groupId); //consumer组名
		props.put("zookeeper.session.timeout.ms", "9000"); //与zookeeper会话超时为9秒
		props.put("zookeeper.sync.time.ms", "200"); //每200毫秒就与zookeeper同步一次
		props.put("auto.commit.interval.ms", "1000"); //每1秒就提交一次消费数据的offset
		props.put("consumer.timeout.ms", "0"); //consumer超时时间为0
//		props.put("auto.offset.reset", "smallest"); //smallest：如果在zookeeper中不存在该groupId，则从最开始读取数据， 否则，从最近开始读取数据
		this.topic = topic;
		this.scheme = scheme;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(STREAM,scheme.getOutputFields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void open(Map map, TopologyContext topologyContext,
			SpoutOutputCollector collector) {
		this.collector = collector;

		/**
		 * 初始化consumer对象
		 */
		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);//tell kafka how many threads we are providing for which topics
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		this.stream = consumerMap.get(topic).get(0).iterator();
	}

	@Override
	public void nextTuple() {
		try {
			if(stream.nonEmpty()){
				//1. 获取一条数据
				byte[] msg = stream.next().message(); 
				
				//2. 解析数据
				List<Object> tuple = scheme.deserialize(msg); 
				
				//3. 发送数据到下一个组件
				if(tuple != null)
					collector.emit(STREAM,tuple); 
			}
		} catch (ConsumerTimeoutException e) {
			//consumer不停从kafka中获取数据，如果kafka暂时还没有最新数据，则抛出此异常，所以在这里不做处理
			
		} catch (Exception e){
			LOG.error("error to emit data", e);
		}
	}
	
	@Override
	public void close() {
		/**
		 * 提交消费kafka数据情况，即offset
		 */
		consumer.commitOffsets();
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(Object o) {
		// We don't need to do anything here, Kafka doesn't require acks
	}

	@Override
	public void fail(Object o) {
		// TODO: Deal with failure
	}
}