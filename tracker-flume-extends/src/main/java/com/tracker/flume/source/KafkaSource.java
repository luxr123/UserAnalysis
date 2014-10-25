package com.tracker.flume.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource extends AbstractSource implements Configurable, PollableSource {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);

	private String topic;
	private ConsumerConnector consumer;
	private ConsumerIterator<byte[], byte[]> it;

	@Override
	public void configure(Context context) {

		topic = context.getString("topic");
		if (topic == null) {
			throw new ConfigurationException("Kafka topic must be specified.");
		}

		try {
			this.consumer = KafkaSourceUtil.getConsumer(context);
		} catch (IOException e) {
			logger.error("IOException occur, {}", e.getMessage());
		} catch (InterruptedException e) {
			logger.error("InterruptedException occur, {}", e.getMessage());
		}

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1)); // tell kafka how many threads we are providing for which topics

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		if (consumerMap == null) {
			throw new ConfigurationException("topicCountMap is null");
		}
		List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
		if (topicList == null || topicList.isEmpty()) {
			throw new ConfigurationException("topicList is null or empty");
		}
		KafkaStream<byte[], byte[]> stream = topicList.get(0);
		it = stream.iterator();

	}

	@Override
	public Status process() throws EventDeliveryException {
		List<Event> eventList = new ArrayList<Event>();
		Event event;
		Map<String, String> headers;
		byte[] bytes;
		try {
			if (it.hasNext()) {
				event = new SimpleEvent();
				headers = new HashMap<String, String>();
				headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
				bytes = it.next().message();
				logger.debug("Message: {}", new String());
				event.setBody(bytes);
				event.setHeaders(headers);
				eventList.add(event);
			}
			getChannelProcessor().processEventBatch(eventList);
			return Status.READY;
		} catch (Exception e) {
			logger.error("KafkaSource EXCEPTION, {}", e.getMessage());
			return Status.BACKOFF;
		}
	}

	@Override
	public synchronized void stop() {
		consumer.commitOffsets();
		consumer.shutdown();
		super.stop();
	}
}
