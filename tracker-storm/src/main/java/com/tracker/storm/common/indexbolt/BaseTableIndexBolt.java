package com.tracker.storm.common.indexbolt;
import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.StringUtil;
import com.tracker.common.zookeeper.ZookeeperProxy;
import com.tracker.storm.common.DeliverBolt;
import com.tracker.storm.common.DeliverDBBolt;
/**
 * 
 * 文件名：BaseTableIndexBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:46:50
 * 功能描述：为基础表创建位置索引.这里是用zookeeper记录
 * 信息,因此没有继承DeliverDBBolt类.bolt在信息记录保存
 * 到基础表后接受处理传入的记录行健,时间戳并保存到本地
 * 缓存中,每两秒(可配置)刷新数据到zookeeper中.
 *
 */
public class BaseTableIndexBolt extends DeliverBolt implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(DeliverDBBolt.class);
	private FIELDS m_fields[] = {UserVisitLogFields.FIELDS.webId,UserVisitLogFields.FIELDS.userType,
			UserVisitLogFields.FIELDS.serverLogTime};
	/**
	 * 
	 */
	private static class Signal_Obj implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 5879467118828380849L;
		private int m_count ;
		private String m_workPath;
		public Signal_Obj(){
			m_count = 0;
			m_workPath = null;
		}
		public void plus(){
			m_count++;
		}
		public void setPath(String workPath){
			m_workPath = workPath;
		}
		public void setData(String workPath,int count){
			m_workPath = workPath;
			m_count = count;
		}
		public int getCount(){
			return m_count;
		}
		public String getPath(){
			return m_workPath;
		}
	}
	
	private static final long serialVersionUID = -2301347453617586463L;
	private ZookeeperProxy m_zookeeper ;
	private String m_zkAddress;
	private Long m_startTime;
	private String m_localKey;
	private String m_preKey;
	private Thread m_thread;
	private Signal_Obj m_signal;
	private static String ZK_WORKPATH= "/tracker/position";
	private static Long THWENTYFOURHOURS=60L*60*24*1000;
	private static String INDEX= "index";
	private static Integer NUMBEROFPERNODE = 500;
	private static Long WAITMIN=60L*1*1000;

	public BaseTableIndexBolt(String  zkAddress,String streamId) {
		super(100, streamId);
		// TODO Auto-generated constructor stub
		m_zkAddress = zkAddress;
		m_startTime = 0L;
		m_localKey = null;
		m_preKey = null;
		m_signal = new Signal_Obj();
	}
	
	public static long parseTimeToLong(String original) {
		try {
			DateFormat dateFormat = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			return dateFormat.parse(original).getTime();
		} catch (ParseException e) {
		}
		return System.currentTimeMillis();
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		m_startTime = null;//initial value
		m_thread = new Thread(this);
		m_thread.start();
		if(m_zkAddress != null && !m_zkAddress.equals("")){
			m_zookeeper = new ZookeeperProxy(m_zkAddress + ZK_WORKPATH);
			try {
				m_zookeeper.init();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

//	@Override
//	public void execute(Tuple input) {
//		// TODO Auto-generated method stub
//		int reTryCount = 3;
//		boolean isRecordInNode = false;
//		String key = input.getStringByField("mainkey");
//		Long  serverLogTime= Long.parseLong(input.getStringByField(FIELDS.serverLogTime.toString()));
//		String currentDay = StringUtil.getDayByMillis(serverLogTime);
//		String datePath = ZookeeperProxy.constructZKPath(currentDay);
//		String rowKey = null;
//		if(m_startTime == null || (serverLogTime > m_startTime && (serverLogTime - m_startTime) > THWENTYFOURHOURS)){
//			m_startTime = parseTimeToLong(StringUtil.getCurrentDay() + " 00:00:00");
//			//create new directories
//			rowKey = constructRowKey(1, INDEX);
//		}
//		if(m_currentTime==null || serverLogTime < m_currentTime){
//			isRecordInNode = true;
//			m_currentTime = serverLogTime;
//			m_localKey = key;
//		}else if(serverLogTime == m_currentTime){
//			isRecordInNode = true;
//			if(m_localKey == null)
//				m_localKey = key;
//			else if(m_localKey.compareTo(key) < 0)
//				m_localKey = key;
//			else{
//				isRecordInNode = false;
//			}
//		}
//		//get the work rowkKey
//		reTryCount = 6;
//		do{
//			byte [] tmp = m_zookeeper.getNodeData(datePath);
//			if(tmp != null){
//				rowKey = Bytes.toString(tmp);
//				break;
//			}else{
//				//only one node will success
//				isRecordInNode= true;
//				if(m_zookeeper.record(datePath,rowKey,null)){
//					break;
//				}
//			}
//			if(reTryCount-- > 0){
//				try {
//					Thread.sleep(100);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					continue;
//				}
//			}else{
//				return ;
//			}
//		}while(true);
//		String workPath = ZookeeperProxy.constructZKPath(currentDay,rowKey);
//		byte[] tmp = m_zookeeper.getNodeData(workPath);
//		int localCount = Integer.parseInt(Bytes.toString(tmp));
//		//add count test
//		try{
//			if(tmp == null ){
//				m_zookeeper.increament(workPath, null);
//			}else if( localCount  < NUMBEROFPERNODE){
//				//increament in local
//					m_zookeeper.increament(workPath,null);
//			}else {
//				//update the count
//				isRecordInNode= true;
//				workPath = createNewRecord(rowKey,datePath,key,serverLogTime);
//				if(workPath == null){
//					logger.error("error in create new record for baseTable index.rowkey is " + key);
//					return;
//				}else{
//					m_zookeeper.increament(workPath, null);
//				}
//			}
//		}catch(Exception e){
//			logger.warn("exception happened in add or create node " + rowKey + "\t" + key);
//		}
//		if(isRecordInNode){
//			//record the start row
//			workPath = ZookeeperProxy.constructZKPath(workPath,m_localKey);
//			m_zookeeper.record(workPath, "", null);
//			isRecordInNode = false;
//			m_currentTime = serverLogTime;
//		}
//	}
	/**
	 * 首先取出数据记录的时间戳,对比m_startTime,检查到跨天时更新m_startTime,m_startTime初始值
	 * 为当天凌晨毫秒值.取得当前的日期date,从zookeeper的date节点取出更新信息data,解析,取出时间戳范
	 * 围进行对比,
	 * 主线程:
	 * 1当小于范围时,从data中获得获得上一个工作节点preNode,对preNode的计数加1,如果该行健字典顺序大
	 * 于本地缓存的行健的话,在preNode节点下插入当前记录的基础表行健,同时更新本地缓存的行健.
	 * 2当时间戳在此范围内,或者大于该范围值,从data中获得当前的工作节点curNode,计数到本地,如果缓存计数
	 * 大于NUMBEROFPERNODE时,通知更新线程进行更新
	 * 3当时间戳大于时间范围时,需要更新date节点下的更新信息data.只有当curNode的计数大于10*NUMBEROFPERNODE
	 * 的值时,创建新的节点为curNode,当前curNode变为preNode,同时更新时间范围为上一个范围的最大值preMax-
	 * max(信息时间戳,preMax+1分钟).向新节点插入当前的行健,并缓存该行健.
	 * 4当时间戳大于时间范围时,curNodede的计数小于10*NUMBEROFPERNODE,仅更新时间范围上一个范围的最小值-(信息时间戳,
	 * preMax+1分钟).当前行健大于本地缓存行健时,插入curNode,更新当前行健.
	 *  更新线程:
	 *  1.每两秒(可配置)刷新缓冲区中的数据到zookeeper
	 *  2.接受到主线程的刷新请求时,刷新缓冲区中的数据到zookeeper
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		int reTryCount = 3;
		boolean isRecordInNode = false;
		boolean shoudCompare = true;
		long timeStamp = 0L;
		String key = input.getStringByField("mainkey");
		Long  serverLogTime= Long.parseLong(input.getStringByField(FIELDS.serverLogTime.toString()));
		String currentDay = StringUtil.getDayByMillis(serverLogTime);
		String datePath = ZookeeperProxy.constructZKPath(currentDay); 
		String rowKey = null;
		//检查设置当前的日期
		if(m_startTime == null || (serverLogTime > m_startTime && (serverLogTime - m_startTime) > THWENTYFOURHOURS)){
			m_startTime = parseTimeToLong(StringUtil.getCurrentDay() + " 00:00:00");
			//create new directories
			rowKey = constructRowKey(1, INDEX);
		}
		//设置重试的次数
		//从日期节点取出当前的节点-节点开始时间-节点结束时间-上一个节点
		//如果记录的时间戳小于等于节点开始时间，取上一个节点为工作目录，对目录值加一，加入mainkey到目录
		//如果时间戳小于等于节点结束时间，取当前节点为工作目录，对本机内的缓存加一，当本机的缓存大于200时，刷写到zk上
		//如果时间戳大于节点结束时间，取当前节点为工作目录，刷写数据到zk上，创建新的工作目录，将mainkey添加到新的目录下
		reTryCount = 6;
		boolean preRecord = false;
		boolean updateCount = false;
		long startTime = 0L,endTime = 0L;
		do{
			if(reTryCount > 0 && reTryCount-- < 6){
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					continue;
				}
			}else if(reTryCount <= 0){
				return ;
			}
			byte [] tmp = m_zookeeper.getNodeData(datePath);
			if(tmp != null){
				String splits[] = Bytes.toString(tmp).split(StringUtil.ARUGEMENT_SPLIT);
				if(splits.length < 4){
					continue;
				}
				rowKey = splits[0] + StringUtil.ARUGEMENT_SPLIT + splits[1];
				startTime = Long.parseLong(splits[2]);
				endTime = Long.parseLong(splits[3]);
				if(serverLogTime <= startTime && startTime > 0){
					if(serverLogTime == startTime)
						shoudCompare = true;
					else
						shoudCompare = false;
					preRecord = true;
					if(splits.length == 6)
						rowKey=splits[4] + StringUtil.ARUGEMENT_SPLIT + splits[5];
					break;
				}else if(serverLogTime <= endTime){
					break;
				}else{
					//强制刷新之前的值，创建新的目录
					updateCount = true;
					timeStamp = endTime;
					break;
				}
			}else{
				//only one node will success
				isRecordInNode= true;
				preRecord = true;
				if(m_zookeeper.record(datePath,rowKey + StringUtil.ARUGEMENT_SPLIT + 0 +
							StringUtil.ARUGEMENT_SPLIT + (serverLogTime + WAITMIN),null)){
					timeStamp = serverLogTime;
					break;
				}
			}
		}while(true);
		//构造工作路径
		String workPath = ZookeeperProxy.constructZKPath(currentDay,rowKey);
		//判断工作路径
		if(preRecord){
			m_zookeeper.increament(workPath,null);
			if(m_preKey == null || (shoudCompare && key.compareTo(m_preKey) < 0)){
				m_preKey= key;
				workPath = ZookeeperProxy.constructZKPath(workPath,key);
				m_zookeeper.record(workPath, "", null);
			}
		}else{
			String newWorkPath = "";
			boolean shoudRecord = true;
			try{
				if(updateCount){
					Notify();
				}else{
					ModifyCount(workPath);
					if( getCount() >= NUMBEROFPERNODE){
						Notify();
					}
				}
				if(updateCount){
					//update the count
					isRecordInNode= true;
					int localCount = 0;
					byte tmp[] = m_zookeeper.getNodeData(workPath);
					if(tmp != null)
						localCount = Integer.parseInt(Bytes.toString(tmp));
					newWorkPath = createNewRecord(rowKey,datePath,key,startTime,timeStamp,serverLogTime,localCount);
					if(newWorkPath == null){
						logger.error("error in create new record for baseTable index.rowkey is " + key);
						return;
					}
					if(newWorkPath.equals("") ){
						shoudRecord = false;
					}else{
						shoudRecord = true;
						workPath = newWorkPath;
					}
					ModifyCount(workPath);
				}
			}catch(Exception e){
				logger.warn("exception happened in add or create node " + rowKey + "\t" + key);
			}
			if(shoudRecord && isRecordInNode && workPath != null){
				//record the start row
				if(m_localKey == null || key.compareTo(m_localKey) < 0){
					m_localKey = key;
					workPath = ZookeeperProxy.constructZKPath(workPath,key);
					m_zookeeper.record(workPath, "", null);
				}
				isRecordInNode = false;
			}
		}
	}
	
	@Override
	public List<String> getInputFields() {
		// TODO Auto-generated method stub
		List<FIELDS> list = new ArrayList<FIELDS>(Arrays.asList(m_fields));
		List<String> retVal = UserVisitLogFields.castToList(list);
		retVal.add("mainkey");
		return retVal;
	}
	//创建新的目录成功时
	//如果当前目录的值小于2000时，日期目录下的值为，当前目录-当前目录开始时间-当前目录开始时间+5分钟-当前目录
	//如果当前目录的值大于2000时，日期目录下的值为，新建目录-当前目录开始时间-当前目录开始时间+5分钟-当前目录
	//创建新的目录失败时
	//取日期目录下的值，如果当前目录结束时间>日期值下的开始时间，更新日期目录下的值为：新建目录-当前目录开始时间-当前目录开始时间+5分钟-当前目录
	public String createNewRecord(String usedRowKey,String datePath,String key,long indexStartTime,long indexEndTime,long recordTime,int count){
		try{
			int index = Integer.parseInt(usedRowKey.substring(0, usedRowKey.
					indexOf(StringUtil.ARUGEMENT_SPLIT)));
			int pos = index + 1; 
			String tmpKey = constructRowKey(pos, INDEX);
			String workPath = ZookeeperProxy.constructZKPath(datePath,tmpKey);
			long startTime = indexEndTime;
			long endTime = recordTime;
			//create new record
			
			//update record index
			if(count <= NUMBEROFPERNODE * 10){
				//update start,end time
				//do not move current record to the next record
				tmpKey = usedRowKey;
				usedRowKey = constructRowKey(index == 1 ? index:index-1, INDEX);
				startTime = indexStartTime;
				workPath = "";
			}else{
				usedRowKey = constructRowKey(index, INDEX);
				m_zookeeper.record(workPath, "0", null);
			}
			Stat stat = new Stat();
			byte tmp[] = m_zookeeper.getNodeData(datePath,stat);
			String splits[] = Bytes.toString(tmp).split(StringUtil.ARUGEMENT_SPLIT);
			int reTryCount = 5;
			long tmpTime = 0L;
			tmpTime = Long.parseLong(splits[3]);
			while(reTryCount-- > 0 && recordTime > tmpTime && !m_zookeeper.update(datePath,
					constructRecordInfo(tmpKey,usedRowKey,startTime,endTime),stat)){
				try{
					Thread.sleep(50);
					tmp = m_zookeeper.getNodeData(datePath,stat);
					if(tmp == null)
						continue;
					splits = Bytes.toString(tmp).split(StringUtil.ARUGEMENT_SPLIT);
					if(splits.length == 6)
						tmpTime = Long.parseLong(splits[3]);
				}catch (Exception e) {
					// TODO: handle exception
					logger.error("error in creatNewRecord while get datePate value = " + Bytes.toString(tmp) );
					break;
				}
			};
			return workPath;
		}catch(Exception e){
			logger.warn("error in createNewRecord");
			return null;
		}
		
	}
	
	public static String getCompentId(){
		return "BaseTableIndex";
	}
	
	public static String constructRowKey(int number,String... rowKeys){
		String tmp = number+StringUtil.ARUGEMENT_SPLIT + rowKeys[0];
		for(int i = 1;i<rowKeys.length;i++){
			tmp+= StringUtil.ARUGEMENT_SPLIT + rowKeys[i];
		}
		return tmp;
	}

	private String constructRecordInfo(String curRowKey,String preRowKey,long startTime,long endTime){
		if(startTime > 0 && endTime < startTime + WAITMIN)
			endTime = startTime + WAITMIN;
		else if(startTime == 0){
			endTime += WAITMIN;
		}
		return curRowKey + StringUtil.ARUGEMENT_SPLIT + startTime 
			+ StringUtil.ARUGEMENT_SPLIT + endTime + StringUtil.ARUGEMENT_SPLIT + preRowKey; 
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			synchronized (m_signal) {
				try {
					m_signal.wait(1000);
					if(m_signal.getCount() <= 0  || m_signal.getPath() == null || m_signal.getPath().equals("")){
						continue;
					}else{
						m_zookeeper.increament(m_signal.getPath(),Integer.toString(m_signal.getCount()),null);
						m_signal.setData(null, 0);
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
				}
			}
		}
	}
	
	private void Notify(){
		synchronized (m_signal) {
			m_signal.notify();
		}
	}
	
	private int getCount(){
		synchronized(m_signal){
			return m_signal.getCount();
		}
	}
	
	private void ModifyCount(String workPath){
		synchronized (m_signal) {
			m_signal.plus();
			m_signal.setPath(workPath);
		}
	}
	
}
