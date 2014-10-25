package com.tracker.data;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.tracker.data.Servers;
import com.tracker.db.hbase.HbaseUtils;

public class GenerateStats {
	public void initHBaseTable(String tableName, String ...cfs) throws IOException {
		HBaseAdmin hbaseAdmin = HbaseUtils.getHBaseAdmin(Servers.ZOOKEEPER);
		// delete and init d_referrer
		if (hbaseAdmin.tableExists(tableName)) {
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);// 建表
		for(String cf : cfs){
			tableDescriptor.addFamily(new HColumnDescriptor(cf));// 创建列族
		}
		hbaseAdmin.createTable(tableDescriptor);
		hbaseAdmin.close();
	}
}
