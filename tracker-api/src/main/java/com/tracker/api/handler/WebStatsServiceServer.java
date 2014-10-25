package com.tracker.api.handler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.api.Servers;
import com.tracker.api.thrift.data.DataService;
import com.tracker.api.thrift.search.SearchStatsService;
import com.tracker.api.thrift.web.WebStatsService;

/**
 * 对外提供服务的启动类,采用thrift来声明服务，提供网站统计、站内搜索统计以及一些数据服务。
 * 
 * Thrift是一个跨语言的服务部署框架，最初由Facebook于2007年开发，2008年进入Apache开源项目。Thrift通过一个中间语言(IDL, 接口定义语言)来定义RPC的接口和数据类型，
 * 然后通过一个编译器生成不同语言的代码（目前支持C++,Java, Python, PHP, Ruby, Erlang, Perl, Haskell, C#, Cocoa, Smalltalk和OCaml）,并由生成的代码负责RPC协议层和传输层的实现。
 * 
 * @author jason.hua
 */
public class WebStatsServiceServer {
	private static Logger logger = LoggerFactory.getLogger(WebStatsServiceServer.class);
	
	/**
	 * main方法，主控进程
	 * @param args
	 */
	public static void main(String[] args){
		WebStatsServiceHandler handler = new WebStatsServiceHandler();
		
		//注册WebSiteService, DataService, SearchStatsService服务
		TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor(); // 一个端口可以对应多个service
		multiProcessor.registerProcessor("WebStatsService", new WebStatsService.Processor<WebStatsServiceHandler>(handler));
		multiProcessor.registerProcessor("DataService", new DataService.Processor<DataServiceHandler>(new DataServiceHandler()));
		multiProcessor.registerProcessor("SearchStatsService", new SearchStatsService.Processor<SearchStatsServiceHandler>(new SearchStatsServiceHandler()));
		
		final int port = Servers.webStatsThriftPort; //端口号
		try {
			//配置服务
			InetAddress address = InetAddress.getLocalHost(); //地址信息
			TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(new InetSocketAddress(address.getHostAddress(), port));
			THsHaServer.Args thhsArgs = new THsHaServer.Args(serverTransport); //设置服务地址和端口号
			thhsArgs.processor(multiProcessor); //设置需要声明的处理类
			thhsArgs.transportFactory(new TFramedTransport.Factory()); //使用非阻塞io
			thhsArgs.protocolFactory(new TCompactProtocol.Factory()); //使用压缩的二进制格式
			thhsArgs.executorService(Executors.newFixedThreadPool(64)); //处理线程数设为64
			final TServer server = new THsHaServer(thhsArgs);// 半同步半异步的服务模型
			
			//启动服务(使用单个线程启动)
		    ExecutorService servingExecutor = Executors.newSingleThreadExecutor();
		    servingExecutor.submit(new Thread() {
		    	@Override
		    	public void run() {
		    		server.serve();
		    	}
		    });
		    
		    //循环等待服务启动
		    long timeAfterStart = System.currentTimeMillis();
		    while(!server.isServing()) {
		      try {
		        if(System.currentTimeMillis() - timeAfterStart >=10000) {
		          logger.info("WebStatsService, DataService, SearchStatsService failed to start!");
		          break;
		        }
		        TimeUnit.MILLISECONDS.sleep(1000); //睡眠1秒
		      } catch (InterruptedException e) {
		        Thread.currentThread().interrupt();
		        logger.error("Interrupted while waiting for Thrift server to start.", e);
		      }
		    }
		    logger.info("WebStatsService, DataService, SearchStatsService is started, port:" + port);
		    
		    //当服务停止时，stop其余程序
		    Runtime.getRuntime().addShutdownHook(new Thread() {
		        public void run() {
		        	server.stop();
		        	Servers.shutdown();
		        	logger.info("WebStatsService, DataService, SearchStatsService is stopped, port: " + port);
		       }
		    });
		} catch (TTransportException e) {
			logger.error("WebStatsService, DataService, SearchStatsService failed to start!, port:" + port, e);
		} catch (UnknownHostException e) {
			logger.error("WebStatsService, DataService, SearchStatsService failed to start!, port:" + port, e);
		}
	}
}
