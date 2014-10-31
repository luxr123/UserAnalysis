package com.tracker.data.table.create;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {
	
	private void createWebSiteUserSession(HBaseAdmin admin, String tableName) throws IOException{
		deleteTable(admin, tableName);
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("data"));
		columnDesc.setTimeToLive(86400);
		
		desc.addFamily(columnDesc);
		desc.setDurability(Durability.ASYNC_WAL);
		byte[][] splitKeys = new byte[][] {
				Bytes.toBytes("10"),	
				Bytes.toBytes("20"),	
			};
		admin.createTable(desc, splitKeys);
	}
	
	private void createUnSummableKpi(HBaseAdmin admin, String tableName, int ttl) throws IOException{
		deleteTable(admin, tableName);
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("data"));
		columnDesc.setTimeToLive(ttl);
		desc.addFamily(columnDesc);
		desc.setDurability(Durability.ASYNC_WAL);
		desc.addCoprocessor("com.tracker.coprocessor.endpoint.FilterRowCountEndpoint", 
				new Path("hdfs://mycluster/jar/tracker-hbase.jar"), Coprocessor.PRIORITY_USER, null);
		admin.createTable(desc, Bytes.toBytes("web-city"), Bytes.toBytes("web-user"), 6);
	}
	
	private void createSummableKpi(HBaseAdmin admin, String tableName) throws IOException{
		deleteTable(admin, tableName);
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("data"));
		columnDesc.setTimeToLive(86400);
		desc.addFamily(columnDesc);
		desc.setDurability(Durability.ASYNC_WAL);
		
		byte[][] splitKeys = new byte[][] {
			Bytes.toBytes("se"),	
			Bytes.toBytes("sys"),	
		};
		admin.createTable(desc, splitKeys);
	}
	
	private void deleteTable(HBaseAdmin admin, String tableName) throws IOException{
		if (admin.tableExists(tableName.getBytes())) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}
	
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.2.92,10.100.2.93");
		HBaseAdmin admin = new HBaseAdmin(conf);
//		
		CreateTable createTable = new CreateTable();
//		createTable.createWebSiteUserSession(admin, "website_user_session");

//		createTable.createUnSummableKpi(admin, "rt_kpi_unsummable_day", 86400);
//		createTable.createUnSummableKpi(admin, "rt_kpi_unsummable_week", 604800);
//		createTable.createUnSummableKpi(admin, "rt_kpi_unsummable_month", 2678400);
//		createTable.createUnSummableKpi(admin, "rt_kpi_unsummable_year", 31536000);

//		createTable.createSummableKpi(admin, "rt_kpi_summable");

	}
}
