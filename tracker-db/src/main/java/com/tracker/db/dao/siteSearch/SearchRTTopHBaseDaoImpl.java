package com.tracker.db.dao.siteSearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;
import com.tracker.common.utils.DoublePriorityQueue;
import com.tracker.common.utils.StringUtil;
import com.tracker.coprocessor.generated.TopProtos.KV;
import com.tracker.coprocessor.generated.TopProtos.TopResponse;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult;
import com.tracker.db.dao.siteSearch.entity.SearchTopResTimeResult.ResponseTimeRecord;
import com.tracker.db.dao.siteSearch.entity.SearchTopResult;
import com.tracker.db.dao.siteSearch.entity.SearchTopResult.Entry;
import com.tracker.db.dao.siteSearch.entity.SearchValueParam;
import com.tracker.db.hbase.HbaseUtils;

/**
 * top统计
 * @author jason
 *
 */
public class SearchRTTopHBaseDaoImpl implements SearchRTTopDao{
	private static Logger logger = LoggerFactory.getLogger(SearchRTTopHBaseDaoImpl.class);

	private static HBaseDao m_hbaseRTRecord = null;
	private static HBaseDao m_hbaseIP = null;
	private static HBaseDao m_hbaseValue = null;
	private static Random random = new Random();
	private static int CATCH_SIZE = 500;
	
	public SearchRTTopHBaseDaoImpl(HConnection m_hbaseConnection){
		m_hbaseIP = new HBaseDao(m_hbaseConnection, "rt_search_top_ip");
		m_hbaseRTRecord = new HBaseDao(m_hbaseConnection, "rt_search_top_res_time");
		m_hbaseValue = new HBaseDao(m_hbaseConnection, "rt_search_top_value");
	}
	
	@Override
	public void updateMaxRTRecord(String date, String webId,
			Integer seId, Integer searchType, ResponseTimeRecord record) {
		if(m_hbaseRTRecord == null)
			return ;
		String searchType_str = null;
		if(searchType == null)
			searchType_str = "";
		else
			searchType_str = searchType.toString();
		random.setSeed(System.currentTimeMillis());
		int rand = Math.abs(random.nextInt(1000));
		String rowKey = date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
				+ seId + StringUtil.ARUGEMENT_SPLIT + searchType_str + StringUtil.ARUGEMENT_SPLIT 
				+ (Integer.MAX_VALUE - record.getResponseTime()) + StringUtil.ARUGEMENT_SPLIT + record.getCookieId()
				+ StringUtil.ARUGEMENT_SPLIT + rand ;
		try {
			m_hbaseRTRecord.putRow(rowKey,"data".getBytes(), "record".getBytes(), record.toString().getBytes());
		} catch (Exception e) {
			logger.error("write hbase error", e);
		}
	}

	@Override
	public SearchTopResTimeResult getMaxRTRecord(String date, String webId,
			Integer seId, Integer searchType, int startIndex, int offset) {
		if(startIndex>0)
			startIndex--;
		if(startIndex < 0 || offset < 0)
			return null;
		String searchType_str = null;
		if(searchType == null)
			searchType_str = "";
		else
			searchType_str = searchType.toString();
		HTableInterface table = m_hbaseRTRecord.getTable();
		Scan scan = new Scan();
		scan.setBatch(CATCH_SIZE);
		scan.setBatch(startIndex + offset );
		scan.setStartRow((date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
				+ seId + StringUtil.ARUGEMENT_SPLIT + searchType_str).getBytes());
		scan.setStopRow((date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
				+ seId + StringUtil.ARUGEMENT_SPLIT + searchType_str + ".").getBytes());
		List<ResponseTimeRecord> list = new ArrayList<ResponseTimeRecord>();
		Long totalCount = 0L;
		ResultScanner rs = null;
		try {
			rs = table.getScanner(scan);
			while(startIndex > 0){
				startIndex--;
				totalCount++;
				rs.next();
			}
			for(Result result : rs.next(offset)){
				totalCount++;
				byte retValue[] = result.getValue("data".getBytes(), "record".getBytes());
				if(retValue!=null)
					list.add(ResponseTimeRecord.toObj(Bytes.toString(retValue)));
			}
			while(rs.next() != null && totalCount < CATCH_SIZE){
				totalCount++;
			}
		} catch (IOException e) {
			logger.error("error to get top response time", e);
		}finally{
			if(rs != null)
				rs.close();
		}
		SearchTopResTimeResult retVal = new SearchTopResTimeResult();
		retVal.setList(list);
		retVal.setTotalCount(totalCount);
		return retVal;
	}

	@Override
	public void updateMostSearchForIp(String date, String webId,
			Integer seId, Integer searchType, String ip, long searchCount) {
		if(m_hbaseIP == null)
			return ;
		String rowKey = date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
				+ seId + StringUtil.ARUGEMENT_SPLIT + searchType + StringUtil.ARUGEMENT_SPLIT + ip ;
		try {
			m_hbaseIP.incrementColumnValue(rowKey,"data".getBytes(), "record".getBytes(), searchCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public SearchTopResult getMostSearchForIp(String date, String webId,
			Integer seId, Integer searchType, int startIndex, int offset) {
		Scan scan=new Scan();
		String startRowKey =  date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
				+ seId + StringUtil.ARUGEMENT_SPLIT + searchType ;
		scan.setFilter(new PrefixFilter(startRowKey.getBytes()));
		//Add
		DoublePriorityQueue<KV> dp=new DoublePriorityQueue<KV>(startIndex + offset,true);
		List<Entry> retList = new ArrayList<Entry>();
		SearchTopResult retVal = new SearchTopResult();
		int totalCount = 0;
		try {
			Map<byte[],TopResponse> results=m_hbaseIP.GetTop(scan,startIndex + offset);
			if(results.values()==null || results.isEmpty()){
				retVal.setList(retList);
				retVal.setTotalCount(totalCount);
				return null;
			}
			for(TopResponse response:results.values()){
				for(int i=0;i<response.getKvCount();i++){
					KV kv=response.getKv(i);
					dp.add(kv.getValue(), kv);
				}
				totalCount += response.getDataSize();
			}
			List<KV> list = dp.values();
			for(int i = startIndex; i < startIndex + offset && i < list.size(); i++){
				KV kv = list.get(i);
				String sRowkey=kv.getKey();
				long nValue=kv.getValue();
				String sKey=sRowkey.substring(sRowkey.lastIndexOf(StringUtil.ARUGEMENT_SPLIT) + 1
						, sRowkey.length());
				retList.add(new Entry(sKey, nValue));
			}
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		retVal.setList(retList);
		retVal.setTotalCount(totalCount);
		return retVal;
	}

	@Override
	public void updateMostForSearchValue(String date, String webId,
			Integer seId, Integer searchType,
			List<SearchValueParam> params) {
		// TODO Auto-generated method stub
		if(m_hbaseValue == null)
			return ;
		List<Increment> incrs = new ArrayList<Increment>();
		for(SearchValueParam element:params){
			String rowKey = date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
					+ seId + StringUtil.ARUGEMENT_SPLIT + searchType + StringUtil.ARUGEMENT_SPLIT 
					+ element.getSearchConType() + StringUtil.ARUGEMENT_SPLIT + element.getSearchValue();
			Increment incr = new Increment(rowKey.getBytes());
			incr.addColumn("data".getBytes(), "record".getBytes(), element.getSearchCount());
			incrs.add(incr);
		}
		try {
			m_hbaseValue.batch(incrs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("write hbase error");
		}
	}

	@Override
	public SearchTopResult getMostForSearchValue(String date, String webId,
			Integer seId, Integer searchType, Integer searchConType,
			int startIndex, int offset) {
		// TODO Auto-generated method stub
		Scan scan=new Scan();
		String startRowKey = date + StringUtil.ARUGEMENT_SPLIT + webId + StringUtil.ARUGEMENT_SPLIT
				+ seId + StringUtil.ARUGEMENT_SPLIT + searchType + StringUtil.ARUGEMENT_SPLIT 
				+ searchConType;
		scan.setFilter(new PrefixFilter(startRowKey.getBytes()));
		//Add
		DoublePriorityQueue<KV> dp=new DoublePriorityQueue<KV>(startIndex + offset,true);
		List<Entry> retList = new ArrayList<Entry>();
		SearchTopResult retVal = new SearchTopResult();
		int totalCount = 0;
		try {
			Map<byte[],TopResponse> results=m_hbaseValue.GetTop(scan,startIndex + offset);
			if(results.values()==null || results.isEmpty()){
				retVal.setList(retList);
				retVal.setTotalCount(totalCount);
				return null;
			}
			for(TopResponse response:results.values()){
				for(int i=0;i<response.getKvCount();i++){
					KV kv=response.getKv(i);
					dp.add(kv.getValue(), kv);
				}
				totalCount += response.getDataSize();
			}
			
			List<KV> list = dp.values();
			for(int i = startIndex; i < startIndex + offset && i < list.size(); i++){
				KV kv = list.get(i);
				String sRowkey=kv.getKey();
				long nValue=kv.getValue();
				String sKey=sRowkey.substring(sRowkey.lastIndexOf(StringUtil.ARUGEMENT_SPLIT) + 1
						, sRowkey.length());
				retList.add(new Entry(sKey, nValue));
			}
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		retVal.setList(retList);
		retVal.setTotalCount(totalCount);
		return retVal;
	}
	public static void main(String[] args) {
		
		SearchRTTopHBaseDaoImpl sDao=new SearchRTTopHBaseDaoImpl(HbaseUtils.getHConnection("10.100.2.92"));
		SearchTopResult result=null;
		/*sDao.updateMostSearchForIp("2014-09-1515:45:48", "01","spy", 1, "10.100.50.92", 100);
		sDao.updateMostSearchForIp("2014-09-1515:45:48", "01","spy", 1, "10.100.50.93", 99);
		sDao.updateMostSearchForIp("2014-09-1515:45:48", "01","spy", 1, "10.100.50.94", 98);
		sDao.updateMostSearchForIp("2014-09-1515:45:48", "01","spy", 1, "10.100.50.95", 97);
		sDao.updateMostSearchForIp("2014-09-1515:45:48", "01","spy", 1, "10.100.50.96", 96);
		sDao.updateMostSearchForIp("2014-09-1515:45:48", "01","spy", 1, "10.100.50.97", 95);
		result=sDao.getMostSearchForIp("2014-09-1515:45:48", "01", "spy", 1, 0, 20);
		
		
		List<SearchValueParam> sList=new ArrayList<SearchValueParam>();
		SearchValueParam value1=new SearchValueParam("abc","bca",2);
		sList.add(value1);
		SearchValueParam value2=new SearchValueParam("edf","fde",3);
		sList.add(value2);
		SearchValueParam value3=new SearchValueParam("rfc","cfr",4);
		sList.add(value3);
		SearchValueParam value4=new SearchValueParam("xyz","zyx",5);
		sList.add(value4);
		
		sDao.updateMostForSearchValue("2014-09-1515:45:48", "01","spy", 1, sList);
		*/
//		result=sDao.getMaxRTRecord("20140919", "1", "FoxEngine", 1, 0, );
		SearchTopResTimeResult res = sDao.getMaxRTRecord("20140923", "1",1 , 1, 50, 10);
		if(res != null){
			System.out.println(res.getTotalCount());
			for(ResponseTimeRecord element:res.getList()){
				System.out.print(element.getResponseTime() + "\t");
				System.out.print(element.getCookieId() + "\t");
				System.out.print(element.getIp() + "\t");
				System.out.print(element.getSearchParam() + "\t");
				System.out.print(element.getUserId() + "\t");
				System.out.print(element.getSearchTime() + "\t");
				System.out.println();
			}
		}
		else
			System.out.println("no data");
		//20140919-1-FoxEngine-3-2147483502
//		for(Entry entry :result.getList()){
//			logger.info(entry.getField() + " ---- " + entry.getSearchCount());
//		}
	}
}
