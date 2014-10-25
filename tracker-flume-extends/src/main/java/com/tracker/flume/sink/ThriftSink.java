package com.tracker.flume.sink;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import com.alibaba.fastjson.JSONObject;

public class ThriftSink extends AbstractSink implements Configurable {
	private final Lock resetLock = new ReentrantLock();
	private String host;
	private int port;
	private TTransport transport = null;
	private int batch;
	
	public void configure(Context context) {
		host = context.getString("host");
		port = context.getInteger("port");
		batch = context.getInteger("batch");
	}
	
	private int count;
	private long startTime = System.currentTimeMillis();
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		resetLock.lock();
		try {
			tx.begin();
			
			for(int i = 0; i < batch; i++){
				Event event = channel.take();
				if(event == null)
					break;
			}
			tx.commit();
		} catch (Exception e) {
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
		if(transport != null)
			transport.close();
		super.stop();
	}
}
