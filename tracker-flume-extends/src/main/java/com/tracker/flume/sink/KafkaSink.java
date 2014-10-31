package com.tracker.flume.sink;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.utils.CRC20;
import com.tracker.common.utils.IntegerUtil;
import com.tracker.common.utils.JsonUtil;
import com.tracker.flume.kafka.partitioner.ProducerPartitioner;

/**
 * 
 * 文件名：KafkaSink
 * 创建人：kris.chen
 * 创建日期：2014-10-27 上午11:14:48
 * 功能描述：根据规则，发送数据到kafka的sink
 *
 */
public class KafkaSink extends AbstractSink implements Configurable {
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	private int batchSize;
	private Producer<byte[], byte[]> producer;
	private final Lock resetLock = new ReentrantLock();
	private String topic;
	
	@Override
	/**
	 * 配置参数
	 */
	public void configure(Context context) {
		batchSize = context.getInteger("batchSize", 1);
		topic = context.getString("topic");
		log.info("context={}", context.toString());
		Properties props = new Properties();
		Map<String, String> contextMap = context.getParameters();
		for (String key : contextMap.keySet()) {
			if (!key.equals("type") && !key.equals("channel")) {
				props.setProperty(key, context.getString(key));
				log.info("key={},value={}", key, context.getString(key));
			}
		}
		props.put("key.serializer.class", DefaultEncoder.class.getName()); // key serializer
		props.put("serializer.class", DefaultEncoder.class.getName()); // value serializer
		props.put("partitioner.class", ProducerPartitioner.class.getName()); // ProducerPartitioner
		producer = new Producer<byte[], byte[]>(new ProducerConfig(props));

	}
	
	@Override
	/**
	 * 具体的处理方法，对数据根据cookiedId转成int值，再通过分区函数划分到固定的kafka分区中
	 * 根据batchsize进行批处理
	 */
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		resetLock.lock();
		try {
			tx.begin();
			List<KeyedMessage<byte[], byte[]>> batch = Lists.newLinkedList();
			
			for(int i = 0; i < batchSize; i++){
				Event event = channel.take();
				if(event == null)
					continue;
//				Map<String, String> headers = event.getHeaders();
//				Object[] values = headers.values().toArray();
//				if(values == null || values.length == 0 || values[0].toString().length() == 0)
//					continue;
//				String topic = values[0].toString();
//				log.info("topic: + " + topic + " => " + new String(event.getBody()));

				//----add by Kris-----
				//将log通过cookieId转成int值，再通过分区函数划分到固定kafka分区
				Map<String,Object> kv = JsonUtil.parseJSON2Map(new String(event.getBody()));
				Object cookieId = kv.get(ApachePVLog.FIELDS.cookieId.toString());
				if(cookieId != null){
					String sCookieId = cookieId.toString();
					int nKey=CRC20.getId(sCookieId);
					batch.add(new KeyedMessage<byte[], byte[]>(topic, IntegerUtil.toBytes(nKey), event.getBody()));
				} else {
					log.warn("cookieId is null, topic: + " + topic + " => " + new String(event.getBody()));
				}
			}
			int size = batch.size();
			if (size  == 0) {
				status = Status.BACKOFF;
			} else {
				producer.send(batch);
			}
			tx.commit();
			//log.info("-----------------kafkaSink done------------------------");
		} catch (Exception e) {
			log.error("KafkaSink Exception:{}", e);
			tx.rollback();
			status = Status.BACKOFF;
		} finally {
			resetLock.unlock();
			tx.close();
		}
		return status;
	}

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		super.stop();
	}

}
