package com.tracker.storm.kpiStatistic;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.storm.common.StormConfig;
import com.tracker.storm.common.indexbolt.BaseTableIndexBolt;
import com.tracker.storm.common.indexbolt.CookieUserFieldIndexBolt;
import com.tracker.storm.kpiStatistic.bolt.SaveApacheLogBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.KpiUpdateBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.SearchTopStatsBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.SessionTimeOutBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.SummableKpiBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.UnSummableKpiBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.UserSessionBolt;
import com.tracker.storm.kpiStatistic.bolt.kpi.UserStatsBolt;
import com.tracker.storm.kpiStatistic.spout.ApacheLogSpout;
import com.tracker.storm.kpiStatistic.spout.SignalSpout;

/**
 * 
 * 文件名：WebSiteStatisticTopology
 * 创建人：jason.hua
 * 创建日期：2014-10-14 上午10:39:43
 * 功能描述：网站统计拓扑结构，用于统计网站和站内搜索指标，并存储日志记录
 * 
 */
public class WebSiteStatisticTopology {
	
	/**
	 * 函数名：main
	 * 功能描述：主控方法
	 * @param args 传入local，则启动本地模式，否则提交topology到storm集群上
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION"); //从系统变量中获取hdfs路径，hdfs://10.100.2.94
		String configFile = java.lang.System.getenv("COMMON_CONFIG");//从系统变量中获取配置文件路径，/tracker/resource/config.properties
		StormConfig config = new StormConfig(hdfsLocation, configFile);
		
		/**
		 * ==============================设置spout组件================================================================
		 */
		ApacheLogSpout logSpout = new ApacheLogSpout(config.getZookeeper(), "apacheLog", "apacheLog");
		builder.setSpout("apacheLogSpout", logSpout, 1);
		builder.setSpout("signalSpout", new SignalSpout(), 1);
		
		/**
		 * ==============================网站、搜索指标统计（可累加指标、不可累加指标）==============================================
		 */
		builder.setBolt("sessionTimeOutBolt", new SessionTimeOutBolt(config), 1)
				.shuffleGrouping("signalSpout", SignalSpout.STREAMID);
		
		builder.setBolt("userSessionBolt", new UserSessionBolt(config), 2)
				.setNumTasks(4)
				.fieldsGrouping("apacheLogSpout", logSpout.addPVLogStream(UserSessionBolt.getInputFields()), new Fields(ApacheLogSpout.FIELDS.cookieId.toString()))
				.fieldsGrouping("apacheLogSpout", logSpout.addSearchLogStream(UserSessionBolt.getInputFields()), new Fields(ApacheLogSpout.FIELDS.cookieId.toString()))
				.fieldsGrouping("sessionTimeOutBolt", SessionTimeOutBolt.SESSION_TIME_OUT_STREAM, new Fields(SessionTimeOutBolt.FIELDS.cookieId.toString()));
		
		builder.setBolt("summableKpiBolt", new SummableKpiBolt(config), 2)
				.setNumTasks(4)
				.localOrShuffleGrouping("userSessionBolt", UserSessionBolt.SUMMABLE_KPI_STREAM);

		builder.setBolt("userStatsBolt", new UserStatsBolt(config), 2)
				.setNumTasks(4)
				.localOrShuffleGrouping("userSessionBolt", UserSessionBolt.USER_STATS_STREAM);

		builder.setBolt("unSummableKpiBolt", new UnSummableKpiBolt(config), 2)
				.setNumTasks(4)
				.localOrShuffleGrouping("userSessionBolt", UserSessionBolt.UnSUMMABLE_KPI_STREAM);
		
		builder.setBolt("searchTopStatsBolt", new SearchTopStatsBolt(config), 2)
				.setNumTasks(4)
				.localOrShuffleGrouping("unSummableKpiBolt", KpiUpdateBolt.SEARCH_TOP_STREAM);

		/**
		 * ==============================保存日志、建立索引==============================================
		 */
		SaveApacheLogBolt saveLogBolt = new SaveApacheLogBolt(config);
		builder.setBolt("saveApacheLogBolt", saveLogBolt, 2)
				.setNumTasks(4)
				.shuffleGrouping("apacheLogSpout", logSpout.addPVLogStream(saveLogBolt.getInputFields()))
				.shuffleGrouping("apacheLogSpout", logSpout.addSearchLogStream(saveLogBolt.getInputFields()));
		
		//record the index infomation for specied field
		CookieUserFieldIndexBolt fib_ip = new CookieUserFieldIndexBolt(UserVisitLogFields.FIELDS.ip, "ip_index",config.getZookeeper());
		builder.setBolt("index-ip-" + CookieUserFieldIndexBolt.getCompentId(), fib_ip,2)
				.setNumTasks(4)
				.shuffleGrouping("saveApacheLogBolt", saveLogBolt.addStream(fib_ip.getInputFields()));
		
		CookieUserFieldIndexBolt fib_cookie = new CookieUserFieldIndexBolt(UserVisitLogFields.FIELDS.cookieId, "cookie_index",config.getZookeeper());
		builder.setBolt("index-cookieId-" + CookieUserFieldIndexBolt.getCompentId(), fib_cookie,2)
				.setNumTasks(4)
				.shuffleGrouping("saveApacheLogBolt", saveLogBolt.addStream(fib_cookie.getInputFields()));
		
		CookieUserFieldIndexBolt fib_user = new CookieUserFieldIndexBolt(UserVisitLogFields.FIELDS.userId, "user_index",config.getZookeeper());
		builder.setBolt("index-userId-" + CookieUserFieldIndexBolt.getCompentId(), fib_user,2)
				.setNumTasks(4)
				.shuffleGrouping("saveApacheLogBolt", saveLogBolt.addStream(fib_user.getInputFields()));
		
		BaseTableIndexBolt bti = new BaseTableIndexBolt(config.getZookeeper(),"baseTableIndex");
		builder.setBolt(BaseTableIndexBolt.getCompentId(), bti, 2)
				.setNumTasks(4)
				.shuffleGrouping("saveApacheLogBolt", saveLogBolt.addStream(bti.getInputFields()));
//		
		/**
		 * ==============================创建 topology ==============================================
		 */
		// Configuration
		Config conf = new Config();
		conf.setNumWorkers(3);
		//启动本地模式
		if (args.length != 0 && args[0].equals("local")) {
			conf.setDebug(false);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("WebSiteStatsTopology", conf,
					builder.createTopology());
		}
		//启动集群模式
		else {
			try {
				conf.setDebug(false);
				conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,16);
				conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8192);
				StormSubmitter.submitTopology("WebSiteStatsTopology", conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
