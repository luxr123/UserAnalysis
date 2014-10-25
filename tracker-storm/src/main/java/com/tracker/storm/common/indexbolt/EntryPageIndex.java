package com.tracker.storm.common.indexbolt;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.data.Stat;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.log.UserVisitLogFields.INDEX_FIELDS;
import com.tracker.common.utils.StringUtil;
import com.tracker.common.zookeeper.ZookeeperProxy;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;

public class EntryPageIndex extends FieldIndexBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6416879359759468220L;
	private FIELDS m_fields[] = {FIELDS.webId,FIELDS.cookieId,FIELDS.refType,FIELDS.serverLogTime
			,FIELDS.curUrl};
	private ZookeeperProxy m_zookeeper ;
	private String m_zkAddress;
	private Long m_startTime;
	private static String ZK_WORKPATH= "/tracker/pageEntry";
	private static Long THIRTYMIN=60L*30*1000;
	private static Long THWENTYFOURHOURS=60L*60*24*1000;
	
	
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

	public EntryPageIndex(String zkAddress, String table) {
		super(table);
		m_zkAddress = zkAddress;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public List<String> getInputFields() {
		// TODO Auto-generated method stub
		List<FIELDS> list = new ArrayList<FIELDS>(Arrays.asList(m_fields));
		List<String> retVal = UserVisitLogFields.castToList(list);
		retVal.add("mainkey");
		return retVal;
	}
	
	public static String getCompentId(){
		return "EntryPage";
	}
	
	@Override
	public void execute(Tuple input) {
		Integer refType = Integer.parseInt(input.getStringByField(FIELDS.refType.toString()));
		String cookieId = input.getStringByField(FIELDS.cookieId.toString());
		String webId = input.getStringByField(FIELDS.webId.toString());
		String curUrl = input.getStringByField(FIELDS.curUrl.toString());
		Long  serverLogTime = Long.parseLong(input.getStringByField(FIELDS.serverLogTime.toString()));
		String mainKey = input.getStringByField("mainkey");
		if(m_startTime == null || (serverLogTime > m_startTime && (serverLogTime - m_startTime) > THWENTYFOURHOURS)){
			//create new directories
			String date = StringUtil.getDayByMillis(serverLogTime);
			m_zookeeper.record(ZookeeperProxy.constructZKPath(date), "", null);
			m_startTime = parseTimeToLong(StringUtil.getCurrentDay() + " 00:00:00");
		}
		String rowKey = StringUtil.getDayByMillis(serverLogTime)  + StringUtil.PATH_SPLIT + cookieId + 
				StringUtil.ARUGEMENT_SPLIT + webId;
		String workPath = ZookeeperProxy.constructZKPath(rowKey);
		String hbaseKey = cookieId + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT + curUrl 
				+ StringUtil.ARUGEMENT_SPLIT + StringUtil.getDayByMillis(serverLogTime);
		Stat stat = new Stat();
		Long timeStamp = null;
		byte tmp[] = m_zookeeper.getNodeData(workPath,stat);
		if(tmp != null && refType == 1){
			// the session has not create
			boolean result = false;
			do{
				timeStamp = Long.parseLong(new String(tmp));
				if(serverLogTime > timeStamp && (serverLogTime - timeStamp) < THIRTYMIN){
					//update the session time
					result = m_zookeeper.record(workPath, serverLogTime.toString(),stat);
					if(!result){
						tmp = m_zookeeper.getNodeData(workPath,stat);
					}
					else{
						return;
					}
				}else if(serverLogTime <= timeStamp){
					return;
				}else
					break;
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}while(!result);
		}
		// start a new session
		if(m_zookeeper.record(workPath, serverLogTime.toString(), null)){
			recordIndex(hbaseKey,mainKey,serverLogTime);
		}
	}
	private void recordIndex(String rowKey,String mainKey,Long serverLogTime){
		HbaseParam param = new HbaseParam(serverLogTime);
		param.setRowkey(rowKey);
		String qualitify = INDEX_FIELDS.keyList.toString() + "_pv";
		param.addPut(UserVisitLogFields.Index_Family, qualitify, mainKey);
//		qualitify = INDEX_FIELDS.keyList.toString() + "_cookie";
//		param.addPut(UserVisitLogFields.Index_Family,qualitify,mainKey);
		m_hbaseProxy.batchWrite(param, null);
	}
}
