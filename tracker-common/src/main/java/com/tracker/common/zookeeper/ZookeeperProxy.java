package com.tracker.common.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.StringUtil;
/**
 * 
 * 文件名：ZookeeperProxy
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午10:39:21
 * 功能描述：提供读写zookeeper的功能,用于分布式程序的协调工作.对于再分布式程序
 * 中的使用,使用以下约定--1.数据的写入必须提供版本号,2.数据的读取需要先进行同步
 * 操作.
 *
 */
public class ZookeeperProxy implements Watcher{
	private CountDownLatch m_downLatch;
	private static Logger logger = LoggerFactory.getLogger(ZookeeperProxy.class);
	protected ZooKeeper m_zkConn;
	private String m_zkAddress;
	private int m_sessionTimeOut;
	private Watcher m_process;
	private static Integer WAIT_TIME = 100;
	private static int CONNECT_TIMEOUT = 5000;
	private static int SESSION_TIMEOUT = 2000;
	private static int DATA_LIMIT = 1048576;//1M
	
	public ZookeeperProxy(String zkAddress){
		this(zkAddress,null);
	}
	
	public ZookeeperProxy(String zkAddress,Watcher eventProcesser){
		m_downLatch = new CountDownLatch(1);
		m_zkAddress = zkAddress;
		m_sessionTimeOut = SESSION_TIMEOUT;
		m_process = eventProcesser;
	}
	/**
	 * 
	 * 函数名：init
	 * 功能描述：创建zookeeper的连接,由于创建的过程是异步的,因此会柱塞到集群应答已连接
	 * 消息后完成连接的创建
	 * @throws IOException
	 */
	public void init() throws IOException{
		m_zkConn = new ZooKeeper(m_zkAddress, m_sessionTimeOut, this);
		try{
			boolean retVal = m_downLatch.await(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
			if(retVal == false)
				throw new IOException("create connection time out");
		}catch (Exception e) {
			// TODO: handle exception
			if(m_zkConn.getState() != States.CONNECTED){
				throw new IOException("connection interceptered by others");
			}
		}
	}

	@Override
	/**
	 * 在接收到集群发来的已连接信号后,释放CountDownLatch,完成zookeeper的初始化
	 */
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		//process zk handle status
		if(event.getState() == KeeperState.SyncConnected){
			m_downLatch.countDown();
		}
		//add other process for data node event
		if(m_process != null)
			m_process.process(event);
	}
	
	private String createOrSetNode(String absPath,String data,Stat inputStat){
		return createOrSetNodeData(absPath, data,inputStat,false);
	}
	
	/**
	 * 
	 * 函数名：createOrSetNodeData
	 * 功能描述：
	 * @param absPath		zookeeper上的绝对路径
	 * @param data			写入节点值
	 * @param inputStat		写入的版本号,如果版本号为null,回去尝试创建该节点.不为空,但版本号小于集群的节点
	 * 						会写入失败
	 * @param retry			true,再首次失败后会尝试重写,对于创建和指定版本号的操作不要使用.
	 * @return
	 */
	private String createOrSetNodeData(String absPath,String data,Stat inputStat,boolean retry){
		String retVal = "";
		int loopCount = 3;
		Stat stat = null;
		boolean onoff = true;
		if(data == null)
			data = "";
		if(data.length() > DATA_LIMIT){
			return null;
		}
		while(loopCount > 0 && onoff){
			onoff = retry;
			try {
					if (loopCount-- < 3){
						Thread.sleep(300);
					}
					if(inputStat != null )
						stat = inputStat;
					if(stat == null){
						retVal = m_zkConn.create(absPath, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						break;
					}else{
						int version = stat.getVersion();
						m_zkConn.setData(absPath, data.getBytes(), version);
					}
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				switch(e.code()){
					case BADVERSION:
						retVal = null;
						break;
					case NONODE:
						retVal = null;
						break;
					case NODEEXISTS:
						//means the node has create,while try to create the node
						logger.warn("exists path " + absPath);
						retVal = null;
						break;
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				retVal = null;
			}
		}
		return retVal;
	}
	
	public byte[] getNodeData(String path){
		return getNodeData(path, null);
	}
	/**
	 * 
	 * 函数名：getNodeData
	 * 功能描述：先让连接的节点同步上集群的最新数据,接着获取数据.
	 * @param path		需要获取值的路径
	 * @param outStat	获取值的状态信息
	 * @return
	 */
	public byte[] getNodeData(String path,Stat outStat){
		byte[] retVal = null;
		try {
			m_zkConn.sync(path, null, null);
			retVal = m_zkConn.getData(path, null, outStat);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			switch(e.code()){
			case NONODE:
//				logger.warn(path + " is not a exsit node ");
				break;
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
		}catch(IllegalArgumentException e){
			logger.error(e.getMessage() + "\t" + path);
		}
		return retVal;
	}
	/**
	 * 
	 * 函数名：record
	 * 功能描述：	对外提供的节点的创建,或者节点的修改操作,当inputStat为空时,尝试创建节点初始值为data,不为空
	 * 				写入指定版本的数据,与集群版本不一致时会写入失败.当data为空且节点不存在,尝试创建节点初始值
	 * 				为1.
	 * @param workPath
	 * @param data
	 * @param inputStat
	 * @return
	 */
	public boolean record(final String workPath,final String data,final Stat inputStat){
		Stat stat = null;
		String wData = null;
		if(data == null){
			stat = new Stat();
			byte tmp[] = getNodeData(workPath, stat);
			Integer intTmp = 1;
			if(tmp != null)
				return false;
			wData = intTmp.toString();
		}else{
			stat = inputStat;
			wData = data;
		}
		String retVal = createOrSetNode(workPath,wData,stat);
		return retVal == null ? false: true;
	}
	/**
	 * 
	 * 函数名：update
	 * 功能描述：更新操作,当不指定stat时,会从集群上获取最新的版本,向该版本写入值
	 * @param workPath
	 * @param data
	 * @param inputStat
	 * @return
	 */
	public boolean update(final String workPath,final String data,final Stat inputStat){
		Stat stat = null;
		String wData = "";
		if(data != null)
			wData = data;
		if(inputStat == null){
			stat = new Stat();
			getNodeData(workPath, stat);
		}else{
			stat = inputStat;
		}
		String retVal = createOrSetNode(workPath,wData,stat);
		return retVal == null ? false: true;
	}
	
	public void increament(String workPath,Long timeOut){
		increament(workPath,null,timeOut);
	}
	/**
	 * 
	 * 函数名：increament
	 * 功能描述：对指定路径workpath节点上的值加1或者data大小.如果失败,会在timeout时间内重试.
	 * 			 如果节点不存在会创建该节点,初始值为1
	 * @param workPath
	 * @param data
	 * @param timeOut
	 */
	public void increament(String workPath,final String data,Long timeOut){
		Stat stat = new Stat();
		int i = 1;
		String wData = null;
		while(null == timeOut || timeOut > 0){
			byte tmp[] = getNodeData(workPath, stat);
			Integer intTmp = 1;
			if(data != null)
				intTmp = Integer.parseInt(data);
			if(tmp != null){
				intTmp = (Integer.parseInt(new String(tmp)) + intTmp);
			}else{
				stat = null;
			}
			wData = intTmp.toString();
			String retVal = createOrSetNode(workPath, wData,stat);
			if(retVal == null){
				try {
					int sleepTime = i++ * WAIT_TIME;
					if(timeOut != null)
						timeOut -= sleepTime;
					Thread.sleep(sleepTime);
					i = i > 10 ? 10:i;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				}
			}else{
				break;
			}
		}
	}
	/**
	 * 
	 * 函数名：record_block
	 * 功能描述：record的柱塞版本,会在timeout时间内重试.
	 * @param workPath
	 * @param data
	 * @param inputStat
	 * @param timeOut
	 */
	public void record_block(String workPath,final String data,final Stat inputStat,Long timeOut){
		Stat stat = new Stat();
		int i = 1;
		String wData = null;
		while(null == timeOut || timeOut > 0){
			if(data == null){
				byte tmp[] = getNodeData(workPath, stat);
				Integer intTmp = 1;
				if(tmp != null)
					return;
				else{
					stat = null;
				}
				wData = intTmp.toString();
			}else{
				stat = inputStat;
				wData = data;
			}
			String retVal = createOrSetNode(workPath, wData,stat);
			if(retVal == null){
				try {
					int sleepTime = i++ * WAIT_TIME;
					if(timeOut != null)
						timeOut -= sleepTime;
					Thread.sleep(sleepTime);
					i = i > 10 ? 10:i;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				}
			}else{
				break;
			}
		}
	}
	
	private String getWorkPath(String rowKey,Long timeStamp){
		String currentDayNode = ZookeeperProxy.constructZKPath(StringUtil.getDayByMillis(timeStamp));
		String workPath = null;
		try {
			//check currentday of datanode has created
			Stat stat = m_zkConn.exists(currentDayNode, null);
			if(stat == null){
				String zkKey = System.currentTimeMillis() + StringUtil.ARUGEMENT_SPLIT
						+ rowKey;//create the node and set the data
				m_zkConn.create(currentDayNode, zkKey.getBytes(),
						Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			switch(e.code()){
			case NONODE: //for create function
				logger.warn("the parent path is not exist,please check the root path has created and");
				return null;
			case NODEEXISTS: //for create
				//means other data has created the node 
				//get data from the path
				break;
			case BADARGUMENTS://for exists,create function
				logger.warn("bad argument with : " + currentDayNode);
				return null;
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
		}
		//get the data from the path
		byte tmp [] = getNodeData(currentDayNode);
		workPath = new String(tmp);
		return workPath;
	}
	/**
	 * 
	 * 函数名：constructZKPath
	 * 功能描述：用于构造zookeeper格式的路径
	 * @param path
	 * @return
	 */
	public static String constructZKPath(String path){
		if(path.indexOf(StringUtil.PATH_SPLIT) == 0)
			return path;
		else
			return StringUtil.PATH_SPLIT + path;
	}
	/**
	 * 
	 * 函数名：constructZKPath
	 * 功能描述：用于构造zookeeper格式的路径
	 * @param path
	 * @return
	 */
	public static String constructZKPath(String...path){
		String retVal = "";
		if(path[0].indexOf(StringUtil.PATH_SPLIT) != 0){
			path[0] = StringUtil.PATH_SPLIT + path[0];
		}
		for(String element:path)
			retVal +=element + StringUtil.PATH_SPLIT;
		return retVal.substring(0,retVal.length()-1);
	}
	
}
