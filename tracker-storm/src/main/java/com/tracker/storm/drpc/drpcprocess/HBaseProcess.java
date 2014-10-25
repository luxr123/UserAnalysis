package com.tracker.storm.drpc.drpcprocess;


import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;

import scala.actors.threadpool.Arrays;

import com.tracker.common.utils.StringUtil;
import com.tracker.db.hbase.HbaseCRUD;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
import com.tracker.db.hbase.HbaseUtils;
import com.tracker.storm.data.DataService;
import com.tracker.storm.drpc.drpcresult.DrpcResult;
import com.tracker.storm.drpc.drpcresult.SearchValueResult;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem;
import com.tracker.storm.drpc.spottype.DynamicConstruct;
/**
 * 
 * 文件名：HBaseProcess
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:23:39
 * 功能描述：访问hbase功能的drpcProcess类,实现了DynamicConstruct接口用于动态创建hbase连接对象.
 *
 */
public class HBaseProcess extends DrpcProcess implements Serializable,DynamicConstruct{

	/**
	 * 
	 */
	private static final long serialVersionUID = -517360849307802366L;
	protected HbaseCRUD m_crud;
	public static String ProcessFunc = "";
	protected DataService dataService;
	
	public static long parseTimeToLong(String original) {
		try {
			DateFormat dateFormat = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			return dateFormat.parse(original).getTime();
		} catch (ParseException e) {
		}
		return System.currentTimeMillis();
	}
	
	public HBaseProcess(){
		m_crud = null;
	}
	
	public HBaseProcess(String tableName,String zookeeper){
		m_crud = null;
		Object[] tmp = new Object[2];
		tmp[0] = tableName;
		tmp[1] = zookeeper;
		init(tmp);
	}
	
	@Override
	public void init(Object[] args) {
		// TODO Auto-generated method stub
		String tableName = (String)args[0];
		String zookeeper = (String)args[1];
		if(tableName != null && zookeeper != null){
			m_crud = new HbaseCRUD(tableName, zookeeper);
		}
		dataService = new DataService(HbaseUtils.getHConnection(zookeeper));
	}
	
	@Override
	public boolean isProcessable(String input) {
		// TODO Auto-generated method stub
		//topsearchvalue:engine:searchtype:startindex:endindex
		return true;
	}

	@Override
	public DrpcResult process(String input, Object localbuff) {
		// TODO Auto-generated method stub
		//input engine:searchtype:field:startindex:endindex:partlist
//		String splits[] = input.split(StringUtil.ARUGEMENT_SPLIT);
//		Calendar cal = Calendar.getInstance();
//		String prefixKey = (cal.get(Calendar.MONTH) + 1) + StringUtil.ARUGEMENT_SPLIT 
//				+ cal.get(Calendar.DAY_OF_MONTH) + StringUtil.ARUGEMENT_SPLIT;
//		String field[] = {"count"};
//		for(int i = 1;i<4;i++){
//			prefixKey += splits[i] + StringUtil.ARUGEMENT_SPLIT;
//		}
//		List<ValueItem> listInt = new ArrayList<ValueItem>();
//		Integer startIndex = Integer.parseInt(splits[4]);
//		Integer endIndex = Integer.parseInt(splits[5])  +  startIndex;
//		String partitionList[] = splits[6].split(StringUtil.KEY_VALUE_SPLIT);
//		for(String element : partitionList){
//			String startKey = prefixKey  +  element + StringUtil.ARUGEMENT_SPLIT;
//			String endKey = prefixKey + element + ".";
//			HbaseParam hp = new HbaseParam();
//			HbaseResult hr = new HbaseResult();
//			hp.setColumns(Arrays.asList(field));
//			String nextKey = null;
//			//get all result from a startKey
//			do{
//				nextKey = m_crud.readRange(hp, hr, startKey,endKey);
//				for(int i=0;i<hr.size();i++){
//					String name = hr.getRowKey(i).replaceAll(startKey, "");
//					listInt.add(new ValueItem(name, Bytes.toLong(
//							hr.getRawValue(i, "infomation", field[0]))));
//				}
//				startKey = nextKey;
//			}while(nextKey != null);
//		}
//		Collections.sort(listInt, new Comparator<ValueItem>() {
//			@Override
//			public int compare(ValueItem o1, ValueItem o2) {
//				// TODO Auto-generated method stub
//				if(o2.getCount() > o1.getCount())
//					return 1;
//				else if(o2 == o1)
//					return 0;
//				else
//					return -1;
//			}
//		});
//		if(startIndex >= listInt.size() || startIndex < 0)
//			return null;
//		else if (endIndex >= listInt.size())
//			endIndex = listInt.size() ;
//		SearchValueResult svr = new SearchValueResult(new ArrayList<ValueItem>(
//				listInt.subList(startIndex, endIndex)),listInt.size());
//		return svr;
		return null;
	}
}
