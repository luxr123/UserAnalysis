package com.tracker.flume.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

import com.google.common.collect.Lists;
import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.common.log.parser.LogParser;
import com.tracker.common.log.parser.LogParser.LogResult;
import com.tracker.common.utils.CRC20;
import com.tracker.common.utils.IntegerUtil;
import com.tracker.common.utils.JsonUtil;
import com.tracker.flume.kafka.partitioner.IntegerEncoder;
import com.tracker.flume.kafka.partitioner.ProducerPartitioner;

public class TestProducer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.100.2.92:9092,10.100.2.93:9092,10.100.2.94:9092");
		props.put("clientId", "TestProducer");
		props.put("producer.type", "sync");
		props.put("request.required.acks", "1"); // 同步发送数据，保证数据已经发送到replica上
		
		
//		props.put("key.serializer.class", IntegerEncoder.class.getName()); // key serializer
//		props.put("serializer.class", "kafka.serializer.StringEncoder"); // value serializer
		
		props.put("key.serializer.class", DefaultEncoder.class.getName()); // key serializer
		props.put("serializer.class", DefaultEncoder.class.getName()); // value serializer
		props.put("partitioner.class", ProducerPartitioner.class.getName());
//		
		ProducerConfig producerConfig = new ProducerConfig(props);
//		Producer<Integer, String> producer = new Producer<Integer, String>(producerConfig);
		Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(producerConfig);
		long time = System.currentTimeMillis();
		List<String> lineList=new ArrayList<String>();
		lineList.add("116.231.113.212 [1/July/2014:09:19:31 +0800] \"GET /tjpv.gif?webId=1&uid=276&utype=2&ckid=1401rg15914yj0972556181&ckct=1401159140&ip=116.231.113.1&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fwebchat.php&tl=%E5%A4%87%E6%B3%A8-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=" + time + "&reftype=2&refd=baidu.com&refsubd=&refkw=testKeyword HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0\"");
		lineList.add("116.231.113.212 [1/July/2014:09:19:32 +0800] \"GET /tjpv.gif?webId=1&uid=276&utype=2&ckid=1401rg15914yj0972556182&ckct=1401159140&ip=116.231.113.2&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fwebchat.php&tl=%E5%A4%87%E6%B3%A8-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=" + time + "&reftype=2&refd=baidu.com&refsubd=&refkw=testKeyword HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0\"");
		lineList.add("116.231.113.212 [1/July/2014:09:19:33 +0800] \"GET /tjpv.gif?webId=1&uid=276&utype=2&ckid=1401rg15914yj0972556183&ckct=1401159140&ip=116.231.113.3&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fwebchat.php&tl=%E5%A4%87%E6%B3%A8-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=" + time + "&reftype=2&refd=baidu.com&refsubd=&refkw=testKeyword HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0\"");
		lineList.add("116.231.113.212 [1/July/2014:09:19:34 +0800] \"GET /tjpv.gif?webId=1&uid=276&utype=2&ckid=1401rg15914yj0972556181&ckct=1401159140&ip=116.231.113.1&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fwebchat.php&tl=%E5%A4%87%E6%B3%A8-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=" + time + "&reftype=2&refd=baidu.com&refsubd=&refkw=testKeyword HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0\"");
		lineList.add("116.231.113.212 [1/July/2014:09:19:35 +0800] \"GET /tjpv.gif?webId=1&uid=276&utype=2&ckid=1401rg15914yj0972556182&ckct=1401159140&ip=116.231.113.2&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fwebchat.php&tl=%E5%A4%87%E6%B3%A8-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=" + time + "&reftype=2&refd=baidu.com&refsubd=&refkw=testKeyword HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0\"");
		lineList.add("116.231.113.212 [1/July/2014:09:19:36 +0800] \"GET /tjpv.gif?webId=1&uid=276&utype=2&ckid=1401rg15914yj0972556183&ckct=1401159140&ip=116.231.113.3&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fsearchmanager.php&u=http%3A%2F%2F10.100.2.67%2Fspy%2Fpublic%2Fspy%2Fwebchat.php&tl=%E5%A4%87%E6%B3%A8-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=" + time + "&reftype=2&refd=baidu.com&refsubd=&refkw=testKeyword HTTP/1.1\" 200 \"Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0\"");
		
		List<KeyedMessage<byte[], byte[]>> batch = Lists.newLinkedList();
		LogParser logParser = new ApacheLogParser();
		for(String line:lineList){
			LogResult result=logParser.parseLog(line);
			Map<String,Object> kv = JsonUtil.parseJSON2Map(result.getLogJson());
			String sCookieId = kv.get("cookieId").toString();
			int nKey=CRC20.getId(sCookieId);
			
			System.out.println(result.getLogJson());
			System.out.println(sCookieId);
			System.out.println(nKey);
			
			batch.add(new KeyedMessage<byte[], byte[]>("testCustomPartition",IntegerUtil.toBytes(nKey) ,result.getLogJson().getBytes()));
		}
		producer.send(batch);
	}
}
