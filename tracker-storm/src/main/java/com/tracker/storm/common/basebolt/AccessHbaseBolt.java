package com.tracker.storm.common.basebolt;

import java.util.Properties;

import com.tracker.common.utils.ConfigExt;
import com.tracker.db.hbase.HbaseCRUD;
/**
 * 
 * 文件名：AccessHbaseBolt
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午4:43:43
 * 功能描述：能够访问hbase的bolt类.
 *
 */
public class AccessHbaseBolt extends BaseBolt {
	protected HbaseCRUD m_hbaseProxy;
	protected String m_tableName;
	protected String m_zookeeper;
	public AccessHbaseBolt(String tablename){
		m_tableName = tablename;
		m_hbaseProxy = null;
		m_zookeeper = null;
	}
	public AccessHbaseBolt(String tablename,String zookeeper){
		m_tableName = tablename;
		m_hbaseProxy = null;
		m_zookeeper = zookeeper;
	}
	public AccessHbaseBolt(){
		this(null);
	}
	public void setTableName(String tableName){
		m_tableName = tableName;
	}
	public void initTable(){
		if(null == m_zookeeper){
			String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
			String configFile = java.lang.System.getenv("COMMON_CONFIG");
			Properties properties = ConfigExt.getProperties(hdfsLocation, configFile);
			m_zookeeper = properties.getProperty("hbase.zookeeper.quorum");
		}
		if(m_tableName != null){
			m_hbaseProxy = new HbaseCRUD(m_tableName,m_zookeeper);
		}
	}
}

