package com.tracker.storm;

import java.io.IOException;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tracker.common.utils.ConfigExt;
import com.tracker.common.utils.RequestUtil;
import com.tracker.common.utils.RequestUtil.RTVisitorReq;
import com.tracker.common.utils.StringUtil;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.drpc.AggregateReturnBolt;
import com.tracker.storm.drpc.SearchRealTimeStatistic;
import com.tracker.storm.drpc.TransportBolt;
import com.tracker.storm.drpc.drpcprocess.RTIpProcess;
import com.tracker.storm.drpc.drpcprocess.RTRecordProcess;
import com.tracker.storm.drpc.drpcprocess.RTUserProcess;
import com.tracker.storm.drpc.drpcprocess.RTVisitorProcess;
import com.tracker.storm.drpc.groupstream.LinePartitionGroup;
import com.tracker.storm.drpc.groupstream.PartitionGroup;
/**
 * 
 * 文件名：DrpcServerTopology
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:04:54
 * 功能描述：提供查询实时访客服务.客户端
 * 建立drpcClient与storm的Drpc服务器建立
 * 连接,发送请求.drpc服务器转发用户的请求
 * 到这个toplogy,经过计算后将结果发送给drpc
 * 服务器,drpc服务器再发送给客户端.
 *
 */
public class DrpcServerTopology {
	/**
	 * 
	 * 函数名：main
	 * 功能描述：传入local函数会启动一个本地drpc服务器,从控制台传入参数接受请求.
	 * @param args
	 */
	public static void main(String[] args){
		TopologyBuilder builder = new TopologyBuilder();
		// get cluster infomation
		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
		String configFile = java.lang.System.getenv("COMMON_CONFIG");
		Properties properties = ConfigExt.getProperties(hdfsLocation, configFile);
		String zookeeper = properties.getProperty(StormConfig.ZOOKEEPER_NAME);
		
		LocalDRPC drpc = null;
		//declare DrpcSpout,LocalDRPC is used for test in IDE
		if (args.length != 0 && args[0].equals("local")) {
			drpc = new LocalDRPC();
			builder.setSpout("drpcspout", new DRPCSpout(RequestUtil.DRPC_NAME, drpc));
		} else{
			DRPCSpout drpcSpout = new DRPCSpout(RequestUtil.DRPC_NAME);
			builder.setSpout("drpcspout", drpcSpout);
		}
		/**
		 * 修改用户请求转发到下一步到bolt进行计算:比如实时访客的请求,根据下一步
		 * bolt的数量复制请求数,再为每个请求分配数据的分区号,最后转发到下一步bolt
		 * 的每个excute上.
		 */
		TransportBolt tsb = new TransportBolt(SearchRealTimeStatistic.getCompentId());
//		PartitionGroup partitionGroup = new PartitionGroup();
		LinePartitionGroup  partitionGroup = new LinePartitionGroup();
		tsb.addTransport(RTVisitorReq.RTVISITOR_FUNC,partitionGroup);
		tsb.addTransport(RTVisitorReq.RTUSER_FUNC,partitionGroup);
		tsb.addTransport(RTVisitorReq.RTIP_FUNC,partitionGroup);
		tsb.addTransport(RTVisitorReq.RTRECORD_FUNC, partitionGroup);
		builder.setBolt("transportBolt",tsb,1).shuffleGrouping("drpcspout");
		
		/**
		 * 用户用户请求计算的bolt:比如实时访客的请求,根据定义好的请求格式,取出需要
		 * 的分区中的数据进行计算,完成之后把数据推送到下一步bolt进行合并.
		 */
		SearchRealTimeStatistic srts = new SearchRealTimeStatistic();
		//add process item
		Object inputArgs[] = {"cookie_index",zookeeper}; //arg1:working table arg2:zookeeper location
		Object userArgs[] = {"user_index",zookeeper};
		Object ipArgs[] = {"ip_index",zookeeper};
		Object recordArgs[] = {"log_website_regions",zookeeper};
		//RTVisitorProcess: real time visitor request server
		srts.addProcessItem(RTVisitorReq.RTVISITOR_FUNC,RTVisitorProcess.class,inputArgs);
		srts.addProcessItem(RTVisitorReq.RTUSER_FUNC,RTUserProcess.class,userArgs);
		srts.addProcessItem(RTVisitorReq.RTIP_FUNC,RTIpProcess.class,ipArgs);
		srts.addProcessItem(RTVisitorReq.RTRECORD_FUNC,RTRecordProcess.class,recordArgs);
		
		BoltDeclarer bdeclare = builder.setBolt(SearchRealTimeStatistic.getCompentId(),srts, 9);
		bdeclare.directGrouping(TransportBolt.getCompentId(), TransportBolt.getStreamId());
		
		/**
		 * 对分布式计算结果进行合并:比如实时访客的请求,对每个传入的结果信息取出结果的总数以及唯一id,
		 * 与id下缓存的数量进行对比,若相等着取出所有结果进行合并,返回给drpc服务器
		 */
		AggregateReturnBolt arBolt = new AggregateReturnBolt(SearchRealTimeStatistic.getCompentId());
		builder.setBolt("return",arBolt,2).fieldsGrouping(SearchRealTimeStatistic.getCompentId(),
				SearchRealTimeStatistic.getStreamId(), new Fields("ids"));
		
		
		// Configuration
		Config conf = new Config();
		conf.setNumWorkers(3);

		// Topology run
		if (args.length != 0 && args[0].equals("local")) {
//			conf.setDebug(true);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("DrpcServer", conf, builder.createTopology());
			/*--------------------------------------------------------------------------------------------
			 *  	SIMULATE DRPC REQUEST
			 * -------------------------------------------------------------------------------------------
			 */
			// input test
			String func = null;
			byte bytes[] = new byte[255];
			int length = 0;
			try {
				while ((length = System.in.read(bytes)) >= 0) {
					func = new String(bytes, 0, length - 1);
					//topsearchvalue
					if(func.contains(RTVisitorProcess.ProcessFunc)){
						//topsearchvalue:webid##engine:searchtype:startindex:endindex
						//topsearchvalue-1FoxEngine-1-posText-0-9
//						RequestUtil.RTVisitorReq.getCookieReq(webId, userFilter.getCookieId(), 
//							userFilter.getUserType(), startIndex, count, date)
						System.out.println(drpc.execute(RequestUtil.DRPC_NAME, RequestUtil.
								RTVisitorReq.getCookieReq("1",0,null,0,1, 10, "2014-10-29")));
					}else if(func.contains(RTRecordProcess.ProcessFunc)){
						System.out.println(drpc.execute(RequestUtil.DRPC_NAME, 
								RequestUtil.RTVisitorReq.getRecordReq("1", 0,1, 10, "2014-10-29")));
					}else{
						System.out.println(drpc.execute(RequestUtil.DRPC_NAME, func));
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			localCluster.shutdown();
		} else {
			try {
				conf.setDebug(false);
				StormSubmitter.submitTopology("DrpcServer", conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
