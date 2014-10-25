package com.tracker.storm.common.indexbolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Row;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.log.UserVisitLogFields.INDEX_FIELDS;
import com.tracker.common.utils.StringUtil;
import com.tracker.common.utils.TableRowKeyCompUtil;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.storm.common.DeliverDBBolt;
/**
 * 
 * 文件名：FieldIndexBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:52:51
 * 功能描述：为任意一个字段创建索引.创建时需要指定
 * 索引字段,这些字段只能是UserVisitLogFields.FIELDS
 * 枚举类.
 *
 */
public class FieldIndexBolt extends DeliverDBBolt {
	private FIELDS m_indexField;
	private FIELDS m_fields[] = {FIELDS.webId,FIELDS.visitType,FIELDS.serverLogTime};
	
	protected FieldIndexBolt(String table){
		m_indexField = null;
		STREAMID = "fields_stream";
		m_tableName = table;
	}
	
	public FieldIndexBolt(FIELDS indexField,String table,String zookeeper){
		m_indexField = indexField;
		STREAMID = "fields_stream";
		m_tableName = table;
		m_zookeeper = zookeeper;
	}
	
	@Override
	public List<String> getInputFields() {
		// TODO Auto-generated method stub
		List<FIELDS> list = new ArrayList<FIELDS>(Arrays.asList(m_fields));
		if(m_indexField != null)
			list.add(m_indexField);
		List<String> retVal = UserVisitLogFields.castToList(list);
		retVal.add("mainkey");
		return retVal;
	}
	
	/**
	 * 添加索引--索引的格式又TableRowKeyCompUtil创建,存储的值为基础表中的行健,
	 * 另外对索引更新访问计数,用户类型访问计数,以及修改索引最近的一次访问类型.
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String fieldKey = input.getStringByField(m_indexField.toString());
		//confirm the visitType
		Integer visitType = input.getIntegerByField(FIELDS.visitType.toString());
		if(fieldKey == null || fieldKey.equals("")){
			return;
		}
		String mainKey = input.getStringByField("mainkey");
		String webId = input.getStringByField(FIELDS.webId.toString());
		Long logTime = Long.parseLong(input.getStringByField(FIELDS.serverLogTime.toString()));
		String date  = StringUtil.getDayByMillis(logTime);
		List<String> appends = new ArrayList<String>();
		if(input.contains(FIELDS.userType.toString())){
			appends.add(input.getStringByField(FIELDS.userType.toString()));
		}
		String rowKey = TableRowKeyCompUtil.getPartitionRowKey(fieldKey, webId, date, appends);
		HbaseParam param = new HbaseParam(logTime);
		param.setRowkey(rowKey);
		String qualitify = INDEX_FIELDS.keyList.toString() + "_" + visitType;
		String count = INDEX_FIELDS.count.toString() + "_" + visitType;
		param.addPut(UserVisitLogFields.Index_Family, qualitify, mainKey);
		param.addPut(UserVisitLogFields.Index_InfoFam, INDEX_FIELDS.visitType.toString(), visitType.toString());
		param.addInc(UserVisitLogFields.Index_InfoFam, INDEX_FIELDS.count.toString(), 1L);
		param.addInc(UserVisitLogFields.Index_InfoFam, count, 1L);
		m_hbaseProxy.batchWrite(param, null);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		m_hbaseProxy.setDefaultColumnFamliy(UserVisitLogFields.Index_InfoFam);
	}
	
	public static String getCompentId(){
		return "fieldindexBolt";
	}
}
