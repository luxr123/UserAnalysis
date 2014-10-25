package com.tracker.data.table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.tracker.common.utils.ResourceLoader;
import com.tracker.data.Servers;
import com.tracker.data.table.searchData.ManagerSearchConType;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.data.model.SiteSearchValue;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.db.simplehbase.request.PutRequest;

public class SearchDataTableInit{
	HBaseDao seDataDao = new HBaseDao(Servers.hbaseConnection, SiteSearchValue.class);
	
	public void initHBaseTable(String tableName) throws IOException {
		HBaseAdmin hbaseAdmin = HbaseUtils.getHBaseAdmin(Servers.ZOOKEEPER);
		// delete and init d_referrer
		if (hbaseAdmin.tableExists(tableName)) {
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);// 建表
		tableDescriptor.addFamily(new HColumnDescriptor("data"));// 创建列族
		hbaseAdmin.createTable(tableDescriptor);
		hbaseAdmin.close();
	}
	
	/**
	 */
	public void loadDict() throws Exception{
		//FoxEngine
		int seId = 1;
		loadDict(seId, ManagerSearchConType.CORE_POS.getType(), "data/search/corepos");
		loadDict(seId, ManagerSearchConType.COMPANY_SIZE.getType(), "data/search/cosize");
		loadDict(seId, ManagerSearchConType.COMPANY_TYPE.getType(), "data/search/cotype");
		loadDict(seId, ManagerSearchConType.DEGREE.getType(), "data/search/degree");
		loadDict(seId, ManagerSearchConType.SEX.getType(), "data/search/gender");
		loadDict(seId, ManagerSearchConType.INDUSTRY.getType(), "data/search/indtype");
		loadDict(seId,ManagerSearchConType.AREA.getType(),  "data/search/jobarea");
		loadDict(seId, ManagerSearchConType.POS_LEVEL.getType(), "data/search/poslevel");
		loadDict(seId, ManagerSearchConType.WORK_YEAR.getType(), "data/search/workyear");
		
		SiteSearchValue data = new SiteSearchValue();
		data.setName_ch("高级人才库");
		seDataDao.putObject(SiteSearchValue.generateRow(seId, ManagerSearchConType.NISSENIORDB.getType(), "0"), data);
		data.setName_ch("全部人才库");
		seDataDao.putObject(SiteSearchValue.generateRow(seId, ManagerSearchConType.NISSENIORDB.getType(), "1"), data);
		
		data.setName_ch("包括");
		seDataDao.putObject(SiteSearchValue.generateRow(seId, ManagerSearchConType.NISCOHIS.getType(), "1"), data);
		data.setName_ch("不包括");
		seDataDao.putObject(SiteSearchValue.generateRow(seId, ManagerSearchConType.NISCOHIS.getType(), "0"), data);
	}
	
	public void loadDict(int seId, int conType, String filePath) throws Exception{
		BufferedReader br = new BufferedReader(new InputStreamReader(ResourceLoader.getFileInputStream(filePath), "UTF-8"));
		String line = null;
		List<PutRequest<SiteSearchValue>> putRequestList = new ArrayList<PutRequest<SiteSearchValue>>();
		while((line = br.readLine()) != null){
			String[] str = line.split("\t");
			SiteSearchValue data = new SiteSearchValue();
			data.setName_ch(str[1]);
			putRequestList.add(new PutRequest<SiteSearchValue>(SiteSearchValue.generateRow(seId, conType, str[0]), data));
		}
		br.close();
		seDataDao.putObjectList(putRequestList);
	}
	
	public static void main(String[] args) throws Exception {
		SearchDataTableInit tableInit = new SearchDataTableInit();
		//初始化对应的hbase表
//		tableInit.initHBaseTable("d_dictionary");

		//加载数据到表中
		tableInit.loadDict();
	
		Servers.shutdown();
	}
}
