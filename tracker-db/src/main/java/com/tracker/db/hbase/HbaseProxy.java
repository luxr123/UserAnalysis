package com.tracker.db.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.ConfigExt;
import com.tracker.common.utils.StringUtil;;
/**
 * 
 * 文件名：HbaseProxy
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:19:28
 * 功能描述：hbase的存取类,提供写的功能
 *
 */
public class HbaseProxy implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6390567920113330681L;
	private static Logger logger = LoggerFactory.getLogger(HbaseProxy.class);
	protected HTable m_table; //表的连接对象
	protected String m_tableName;	//操作的表名
	protected String m_family;	//默认的列族
//	protected Configuration m_conf;

	public HbaseProxy(String tablename,String zookeeper) {
		if (tablename == null)
			return;
//		m_conf = HBaseConfiguration.create();
		m_tableName = tablename;
		try {
			// if m_tableName dose not exisit in hbase?
			HBaseAdmin hba = HbaseUtils.getHBaseAdmin(zookeeper);
			if (hba.tableExists(m_tableName.getBytes())) {
				if (!hba.isTableAvailable(m_tableName.getBytes())) {
					hba.enableTable(m_tableName.getBytes());
				}
			} else {
				logger.error("table does not exist:" + m_tableName);
				throw new IOException();
			}
			HTableDescriptor htd = hba.getTableDescriptor(m_tableName
					.getBytes());
			m_family = htd.getColumnFamilies()[0].getNameAsString();
			m_table = new HTable(hba.getConfiguration(), m_tableName);
			if (m_table == null || m_family == null)
				throw new IOException();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// System.out.println("error with create table or get columns family with tableName:"
			// + m_tableName);
			// e.printStackTrace();
			logger.error(
					"error with create table or get columns family with tableName:"
							+ m_tableName, e);
			m_table = null;
		}
	}
	
	protected void finalize() throws Throwable {
		if(m_table != null)
			m_table.close();
	}

	public boolean writeToHbase(String key, Map<String, String> famquality_value) {
		return writeToHbase(key,famquality_value,System.currentTimeMillis());
	}
	
	public String getFamily(){
		return m_family;
	}
	
	public boolean increamentToHbase(String key,Map<String,String> famquality_value){
		if (m_table == null)
			return false;
		boolean retVal = true;
		byte[] keybytesarray = key.getBytes();
		if (famquality_value != null) {
			String family = m_family;
			String quality = "";
			Increment increment = new Increment(keybytesarray);
			List<String> list = new ArrayList<String>();
			try {
				for (String element : famquality_value.keySet()) {
					StringUtil.split(element, ":", list);
					if (list.get(0) != null && list.get(0) != "") {
						family = list.get(0);
					} else
						family = m_family;
					quality = list.get(1);
					if (quality != null )
						increment.addColumn(family.getBytes(), quality.getBytes(),1);
					list.clear();
				}
				m_table.increment(increment);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				retVal = false;
			} catch (NullPointerException e) {
				logger.error("error in writetohbase  with arguments:"
						+ family + " " + quality,e);
			}
		}
		return retVal;
	}
	/**
	 * 
	 * 函数名：writeToHbase
	 * 功能描述：写入hbase.对传入的famquality_value解析,当没有:号的情况下使用默认列族
	 * @param key
	 * @param famquality_value
	 * @param timeStamp
	 * @return
	 */
	public boolean writeToHbase(String key, Map<String, String> famquality_value,long timeStamp) {
		if (m_table == null)
			return false;
		boolean retVal = true;
		byte[] keybytesarray = key.getBytes();
		if (famquality_value != null) {
			String family = m_family;
			String quality = "";
			Put put = new Put(keybytesarray,timeStamp);
			List<String> list = new ArrayList<String>();
			try {
				for (Entry<String, String> entry : famquality_value.entrySet()) {
					String tmp = entry.getKey();
					StringUtil.split(tmp, ":", list);
					if (list.get(0) != null && list.get(0) != "") {
						family = list.get(0);
					} else
						family = m_family;
					quality = list.get(1);
					if (quality != null && entry.getValue() != null)
						put.add(family.getBytes(), quality.getBytes(), entry
								.getValue().getBytes());
					list.clear();
				}
				m_table.put(put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				retVal = false;
			} catch (NullPointerException e) {
				logger.error("error in writetohbase  with arguments:"
						+ family + " " + quality,e);
			}
		} else {
			// add new row without value
			try {
				m_table.incrementColumnValue(key.getBytes(),
						m_family.getBytes(), "count".getBytes(), 1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				retVal = false;
			} catch (Exception e) {
				logger.error("write to Hbase error  with arguments:"
						+ key + " " + m_family,e);
			}
		}
		return retVal;
	}

}