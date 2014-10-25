package com.tracker.flume.test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

import com.google.common.collect.Lists;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.log.ApacheSearchLog;
import com.tracker.common.log.parser.ApacheLogParser;
import com.tracker.common.log.parser.LogParser;
import com.tracker.common.log.parser.LogParser.LogResult;
import com.tracker.common.utils.CRC20;
import com.tracker.common.utils.IPUtil;
import com.tracker.common.utils.IntegerUtil;
import com.tracker.common.utils.JsonUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.flume.kafka.partitioner.ProducerPartitioner;

public class TestProducer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.100.2.92:9092,10.100.2.93:9092,10.100.2.94:9092");
		props.put("clientId", "TestProducer");
		props.put("producer.type", "sync");
		props.put("request.required.acks", "1"); // 同步发送数据，保证数据已经发送到replica上
		props.put("key.serializer.class", DefaultEncoder.class.getName()); // key serializer
		props.put("serializer.class", DefaultEncoder.class.getName()); // value serializer
		props.put("partitioner.class", ProducerPartitioner.class.getName());

		
		ProducerConfig producerConfig = new ProducerConfig(props);
		Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(producerConfig);
		long time = System.currentTimeMillis();
		String 	log = "10.100.10.113 [10/Oct/2014:13:29:56 +0800] \"GET /tjpv.gif?webId=1&uid=142&utype=2&ckid=1407133372024196298&ckct=&ip=10.100.50.34&cd=24&ck=true&la=zh-CN&sc=1440x900&re=http%3A%2F%2Fjy.51job.com%2Fspy%2Fwebchat.php&u=http%3A%2F%2Fjy.51job.com%2Fmanager%2Fcv.php%3Fact%3DshowCv%26managerId%3D1500092240%26caseid%3D0%26isenglish%3D0%26passkey%3D0daf7aced626306d53d6a2367760cbd6&tl=testapple2(1500092240)-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1412919064797&reftype=1 HTTP/1.0\" 200 \"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0\"";
		log = "10.100.10.113 [17/Oct/2014:14:14:46 +0800] \"GET /tjsearch.gif?webId=1&uid=451&utype=2&ckid=1413516898155834561&ckct=1413516898&ip=11.100.50.34&cd=32&ck=true&la=zh-cn&sc=1440x900&u=http%3A%2F%2Fjy.51job.com%2Fspy%2Fsearchcase.php%3Fcasetype%3Dfoxhrcase&tl=%E6%89%BECase-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1413526535467&cat=CaseEngine&type=2&isCallSE=true&caseName=716&alltext=0&prepaid=0&exclusive=0&totalCount=1&resultCount=1&curPage=1 HTTP/1.0\" 200 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; InfoPath.1; .NET4.0C; .NET4.0E)\"";
		log = "10.100.10.111 [10/Oct/2014:15:28:17 +0800] \"GET /tjsearch.gif?webId=1&uid=&utype=3&ckid=1412903623491980526&ckct=1412903623&ip=12.100.2.145&cd=16&ck=true&la=zh-cn&sc=1229x768&u=http%3A%2F%2Fjy.51job.com%2Fspy%2Fsearchmanager.php&tl=%E6%89%BE%E7%B2%BE%E8%8B%B1-%E6%97%A0%E5%BF%A7%E7%B2%BE%E8%8B%B1&vt=1412926165202&cat=FoxEngine&type=1&posText=alert(%22done%22)%3B&niscohis=0&nisseniordb=1&responseTime=1&curPage=1&searchType=1&searchParam=1%101%101%100%109%100%100%100%100%100%100%100%100%100%100%100%101%10100%10alert(%26quot%3Bdone%26quot%3B)%3B%10%10&isCallSE=true HTTP/1.0\" 200 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)\"";

		List<KeyedMessage<byte[], byte[]>> batch = Lists.newLinkedList();
		LogParser logParser = new ApacheLogParser();
		Random rand = new Random();
		for(int i = 0; i < 10000; i++){
			LogResult result=logParser.parseLog(log);
			if(result == null){
				continue;
			}
			
//			ApachePVLog logObj = JsonUtil.toObject(result.getLogJson(), ApachePVLog.class);
			ApacheSearchLog logObj = JsonUtil.toObject(result.getLogJson(), ApacheSearchLog.class);

			logObj.setServerLogTime(time + i);
			logObj.setCookieId(logObj.getCookieId() + (i % 1000));
			logObj.setIp(IPUtil.iplongToIp(IPUtil.ipStrToLong(logObj.getIp()) + i % 1000));
			logObj.setUserType(rand.nextInt(3));
			if(logObj.getUserType() >= 3 || logObj.getUserType() <= 0){
				logObj.userId = null;
				logObj.setUserType(3);
			} else {
				logObj.setUserId((i % 1000 + 1) + "");
			}
			
			int nKey=CRC20.getId(logObj.getCookieId());
			batch.add(new KeyedMessage<byte[], byte[]>("apacheLog",IntegerUtil.toBytes(nKey) , JsonUtil.toJson(logObj).getBytes()));
		}
		producer.send(batch);
	}
}
