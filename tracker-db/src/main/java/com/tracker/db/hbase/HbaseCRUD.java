package com.tracker.db.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tracker.common.utils.StringUtil;

/**
 * 
 * 文件名：HbaseCRUD
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:19:16
 * 功能描述：hbase的存取控制类.提供读的读取的操作,以及参数封装类HbaseParam
 * ,返回结果的封装类HbaseResult
 *
 */

public class HbaseCRUD extends HbaseProxy implements Serializable{
	/**
	 * 
	 */
	private static Logger logger = LoggerFactory.getLogger(HbaseCRUD.class);
	private static final long serialVersionUID = 70638568427015187L;
	protected int m_scanCacheSize;
	private String m_scanStartRow;
	private String m_scanEndRow;

	public HbaseCRUD(String tablename,String zookeeper) {
		super(tablename,zookeeper);
		m_scanCacheSize = 1000;
		m_scanStartRow = null;
		// TODO Auto-generated constructor stub
	}
	
	public void setReturnSize(int size){
		m_scanCacheSize = size;
	}

	public boolean writehbase(HbaseParam param) {
		if (param.m_key == null) {
			System.out.println("row_key is null");
			return false;
		}
		if (param.m_timeStamp != -1)
			return super.writeToHbase(param.m_key, param.m_qualityValue,
					param.m_timeStamp);
		else
			return super.writeToHbase(param.m_key, param.m_qualityValue);
	}
	
	/**
	 * 
	 * 函数名：batchRead
	 * 功能描述：		批量读操作
	 * @param param		提供读取的行健列表,以及版本,过滤器,时间范围等参数
	 * @param hresult	返回的结果值
	 */
	public void batchRead(HbaseParam param, HbaseResult hresult){
		if(m_table == null){
			logger.error("table is null");
			return ;
		}
		try {
			List<Get> list = new ArrayList<Get>();
			for(Get element : param.getReadList()){
				if(element == null)
					continue;
				addColumns(param, element);
				if(param.getMaxVersions() > 0)
					element.setMaxVersions(param.getMaxVersions());
				if(param.getFilter() != null && param.getFilter().getFilters().size() > 0)
					element.setFilter(param.getFilter());
				if(param.m_scanStartTime > 0 ){
					if(param.m_scanEndTime > 0 )
						element.setTimeRange(param.m_scanStartTime, param.m_scanEndTime);
					else
						element.setTimeRange(param.m_scanStartTime, Long.MAX_VALUE);
				}else if(param.m_scanEndTime > 0){
					element.setTimeRange(Long.MIN_VALUE, param.m_scanEndTime);
				}
				list.add(element);
			}
			hresult.addResult(Arrays.asList(m_table.get(list)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * 函数名：batchWrite
	 * 功能描述：批量写操作,包括写数据以及计数器操作
	 * @param param
	 * @param hresult
	 */
	public void batchWrite(HbaseParam param, HbaseResult hresult){
		if(m_table == null){
			logger.error("table is null");
			return ;
		}
		try {
			List<Row> tmp= param.getOperationList();
			if(tmp.size() == 0)
				return;
			m_table.batch(tmp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * 函数名：addColumns
	 * 功能描述：用于扫描时,取指定的列
	 * @param param
	 * @param scan
	 */
	private void addColumns(HbaseParam param,Scan scan){
		String value = null;
		String family = m_family;
		List<String> list = new ArrayList<String>();
		for (String string : param.getColumns()) {
			StringUtil.split(string, ":", list);
			if (list.get(0) != null && !list.get(0).equals("")) {
				family = list.get(0);
			} else
				family = m_family;
			// add column family with quality or not
			if (list.get(1) == null || list.get(1).equals("")) {
				scan.addFamily(family.getBytes());
			} else {
				value = list.get(1);
				scan.addColumn(family.getBytes(), value.getBytes());
			}
			list.clear();
		}
	}
	/**
	 * 
	 * 函数名：addColumns
	 * 功能描述：用于批量读操作时,只取指定的列
	 * @param param
	 * @param get
	 */
	private void addColumns(HbaseParam param,Get get){
		String value = null;
		String family = m_family;
		List<String> list = new ArrayList<String>();
		for (String string : param.getColumns()) {
			StringUtil.split(string, ":", list);
			if (list.get(0) != null && !list.get(0).equals("")) {
				family = list.get(0);
			} else
				family = m_family;
			// add column family with quality or not
			if (list.get(1) == null || list.get(1).equals("")) {
				get.addFamily(family.getBytes());
			} else {
				value = list.get(1);
				get.addColumn(family.getBytes(), value.getBytes());
			}
			list.clear();
		}
	}
	/**
	 * 
	 * 函数名：readhbase
	 * 功能描述：扫描读取操作的实际调用函数
	 * @param param
	 * @param hresult
	 * @return
	 */
	private String readhbase(HbaseParam param, HbaseResult hresult) {
		if (param == null || hresult == null)
			return null;
		if(m_table == null){
			logger.error("table is null");
			return null;
		}
		hresult.clear();
		String retVal = null;
		Scan scan = new Scan();
		//filter the request columns
		addColumns(param,scan);
		// if scan rowfilter
		if (param.getRowKey() != null) {
			param.addFilter(new RowFilter(CompareOp.EQUAL,
					new BinaryComparator(param.getRowKey())));
			scan.setStartRow(param.getRowKey());
		}

		if (param.getFilter().getFilters().size() != 0) {
			scan.setFilter(param.getFilter());
		}
		
		try {
			if(param.m_scanStartTime > 0 ){
				if(param.m_scanEndTime > 0 )
					scan.setTimeRange(param.m_scanStartTime, param.m_scanEndTime);
				else
					scan.setTimeRange(param.m_scanStartTime, Long.MAX_VALUE);
			}else if(param.m_scanEndTime > 0){
				scan.setTimeRange(Long.MIN_VALUE, param.m_scanEndTime);
			}
			if (m_scanStartRow != null)
				scan.setStartRow(m_scanStartRow.getBytes());
			if(m_scanEndRow != null)
				scan.setStopRow(m_scanEndRow.getBytes());
			if(param.getMaxVersions() != 0){
				switch(param.getMaxVersions()){
					case -1:
						scan.setMaxVersions();
						break;
					default:
						scan.setMaxVersions(param.getMaxVersions());
				}
			}
			scan.setCaching(m_scanCacheSize);
			ResultScanner resultScanner = m_table.getScanner(scan);
			Result result = null;
			for (int i = 0; i < m_scanCacheSize; i++) {
				result = resultScanner.next();
				if (result != null)
					hresult.addResult(result);
				else
					break;
			}
			// free the resource which on the server-side
			resultScanner.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (hresult.size() == m_scanCacheSize)
			retVal = hresult.getRowKey(hresult.size() - 1);
		else 
			retVal = null;
		return retVal;
	}
	/**
	 * 
	 * 函数名：readFrom
	 * 功能描述：		从starRow开始读取行
	 * @param param		封装的hbase参数
	 * @param hresult 	存放返回的结果
	 * @param startRow	开始的行健
	 * @return
	 */
	public String readFrom(HbaseParam param, HbaseResult hresult,
			String startRow) {
		String retVal = null;
		m_scanStartRow = startRow;
		retVal = readhbase(param, hresult);
		m_scanStartRow = null;
		if(retVal != null)
			return retVal + 0;
		else
			return null;
	}
	/**
	 * 
	 * 函数名：readRange
	 * 功能描述：		读取startRow-endRow范围内的行
	 * @param param		封装的hbase参数
	 * @param hresult	存放返回的结果
	 * @param startRow	起始行
	 * @param endRow	结束行
	 * @return
	 */
	public String readRange(HbaseParam param, HbaseResult hresult,
			String startRow,String endRow) {
		String retVal = null;
		m_scanStartRow = startRow;
		m_scanEndRow = endRow;
		retVal = readhbase(param, hresult);
		m_scanStartRow = null;
		m_scanEndRow = null;
		if(retVal != null)
			return retVal + 0;
		else
			return null;
	}
	/**
	 * 
	 * 函数名：get
	 * 功能描述：		从表中读取一行
	 * @param param
	 * @param hresult
	 */
	public void get(HbaseParam param, HbaseResult hresult) {
		if(param.getRowKey() == null)
			return;
		String startRow = Bytes.toString(param.getRowKey());
		m_scanStartRow = startRow;
		readhbase(param, hresult);
		m_scanStartRow = null;
	}

	/**
	 * 
	 * 函数名：setDefaultColumnFamliy
	 * 功能描述：		设置参数再没有指定列族的情况下,使用的默认列族
	 * @param cfamliy
	 */
	public void setDefaultColumnFamliy(String cfamliy) {
		m_family = cfamliy;
	}

	/**
	 * 
	 * 文件名：HbaseResult
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月23日 下午5:01:02
	 * 功能描述：hbase操作的返回结果封装类
	 *
	 */
	public static class HbaseResult {
		private List<Result> m_result;
		private Integer m_currentPos;
		public HbaseResult() {
			m_result = new ArrayList<Result>(50);
			m_currentPos = new Integer(0);
		}
		
		public void addResult(Result result) {
			m_result.add(result);
		}
		
		public void addResult(List<Result> results) {
			m_result = new ArrayList<Result>(results);
			m_currentPos = 0;
		}
		/**
		 * 
		 * 函数名：getCurPos
		 * 功能描述： 获取结果集中的当前位置
		 * @return
		 */
		public Integer getCurPos(){
			return m_currentPos;
		}
		/**
		 * 
		 * 函数名：modifyCurPos
		 * 功能描述：	移动结果集中的指针
		 * @param item
		 */
		public void modifyCurPos(Integer item){
			m_currentPos += item;
			while(m_currentPos < m_result.size() && (m_result.get(m_currentPos) == null 
					|| m_result.get(m_currentPos).size() == 0)){
				m_currentPos++;
			}
		}
		/**
		 * 
		 * 函数名：getRowKeys
		 * 功能描述：	返回结果集中所有行的行健
		 * @return
		 */
		public List<String> getRowKeys() {
			List<String> rowKeys = new ArrayList<String>();
			for (Result result : m_result) {
				rowKeys.add(Bytes.toString(result.getRow()));
			}
			return rowKeys;
		}
		/**
		 * 
		 * 函数名：getRaw
		 * 功能描述：	返回第pos行的hbase原始result结果
		 * @param pos
		 * @return
		 */
		public Result getRaw(int pos ){
			return m_result.get(pos);
		}
		/**
		 * 
		 * 函数名：list
		 * 功能描述：	返回整个结果集
		 * @return
		 */
		public List<Result> list(){
			return m_result;
		}
		/**
		 * 
		 * 函数名：addList
		 * 功能描述：	添加结果集
		 * @param addition
		 */
		public void addList(List<Result> addition){
			m_result.addAll(addition);
		}

		/**
		 * 
		 * 函数名：getRowKey
		 * 功能描述：	获取第pos行的行健
		 * @param pos
		 * @return
		 */
		public String getRowKey(int pos) {
			String retVal = null;
			retVal = Bytes.toString(m_result.get(pos).getRow());
			return retVal;
		}
		/**
		 * 
		 * 函数名：getValue
		 * 功能描述：	获取第pos行,family:qualifier单元格下的数值
		 * @param pos	
		 * @param family
		 * @param qualifier
		 * @return
		 */
		public String getValue(int pos,String family,String qualifier){
			if(pos >= m_result.size()){
				return null;
			}
			return Bytes.toString(m_result.get(pos).getValue(family.getBytes(), qualifier.getBytes()));
		}
		/**
		 * 
		 * 函数名：getAllData
		 * 功能描述：		获取第pos行,family:qualString单元格所有版本的数据
		 * @param pos
		 * @param family
		 * @param qualString
		 * @return
		 */
		public List<String> getAllData(int pos,String family,String qualString){
			if(pos < 0 || pos >= m_result.size()){
				return null;
			}
			List<String> retVal = new ArrayList<String>();
			List<Cell> list = m_result.get(pos).getColumnCells(family.getBytes(), qualString.getBytes());
			//TODO
			for(Cell element : list){
				retVal.add(Bytes.toString(element.getValueArray(),element.getValueOffset()
						,element.getValueLength()));
			}
			return retVal;
		}
		/**
		 * 
		 * 函数名：getAllTimeStamp
		 * 功能描述：		获取第pos行,family:qualString单元格所有版本的时间戳(不要调用这个函数)
		 * @param pos
		 * @param family
		 * @param qualString
		 * @param visitType
		 * @return
		 */
		public List<Long> getAllTimeStamp(int pos,String family,String qualString,Integer visitType){
			if(pos < 0 || pos >= m_result.size()){
				return null;
			}
			List<Long> retVal = new ArrayList<Long>();
			List<Cell> list = null;
			if(visitType != null){
				qualString += "_" + visitType;
				list = m_result.get(pos).getColumnCells(family.getBytes(), qualString.getBytes());
			}else{
				return null;
			}
			//TODO
			for(Cell element : list){
				retVal.add(element.getTimestamp());
			}
			return retVal;
		}
		/**
		 * 
		 * 函数名：getAllTimeStamp
		 * 功能描述：		获取第pos行,family:qualString单元格所有版本的时间戳
		 * @param pos
		 * @param family
		 * @param qualString
		 * @param visitType
		 * @return
		 */
		public List<Long> getAllTimeStamp(int pos,String family,String qualString){
			if(pos < 0 || pos >= m_result.size()){
				return null;
			}
			List<Long> retVal = new ArrayList<Long>();
			List<Cell> list = null;
			//TODO
			for(Cell element : list){
				retVal.add(element.getTimestamp());
			}
			return retVal;
		}
		/**
		 * 
		 * 函数名：getTimeStamp
		 * 功能描述：		获取第pos行,family:qualString单元格的时间戳
		 * @param pos
		 * @param family
		 * @param qualifier
		 * @return
		 */
		public Long getTimeStamp(int pos,String family,String qualifier){
			Cell tmp = m_result.get(pos).getColumnLatestCell(family.getBytes(), qualifier.getBytes());
			if(tmp != null)
				return tmp.getTimestamp();
			else
				return null;
		}
		/**
		 * 
		 * 函数名：getRawValue
		 * 功能描述：		获取第pos行的family:qualifier单元的字节结果
		 * @param pos
		 * @param family
		 * @param qualifier
		 * @return
		 */
		public byte[] getRawValue(int pos,String family,String qualifier){
			if(pos < m_result.size() && m_result.get(pos) != null)
				return m_result.get(pos).getValue(family.getBytes(), qualifier.getBytes());
			else
				return null;
		}
		

		public int size() {
			return m_result.size();
		}
		
		public void clear() {
			m_result.clear();
			m_currentPos = 0;
		}
	}
	/**
	 * 
	 * 文件名：HbaseParam
	 * 创建人：zhifeng.zheng
	 * 创建日期：2014年10月23日 下午5:10:25
	 * 功能描述：	hbase操作的参数封装,缓存写一行数据的参数,以及多行或扫描读取的参数
	 *
	 */
	public static class HbaseParam {
		private String m_key;					//用于写操作时,指定行健,用于读操作时,指定读取的行健
		private long m_timeStamp;				//用于写操作时,指定时间戳,用于读操作时,指定读取的版本时间戳
		private Map<String, String> m_qualityValue;	//用于写操作时指定列族:列-值,不存在列族时使用默认列族,也用于读
		private FilterList m_filters;			//用于读操作时使用的过滤器
		private Integer m_maxVersion;			//用于读操作是返回的最大版本数,-1代表全部
		private List<Get> m_gets;				//用于批量读操作的封装
		private Put m_puts;						//用于写操作的封装,目前只支持一行的操作
		private Increment m_incr;				//用于计数器操作的封装,目前只支持一行的操作
		public long m_scanStartTime,m_scanEndTime; //用于扫描读操作时,设置的时间范围
		/**
		 * 
		 * 函数名：getReadList
		 * 功能描述：返回批量读的列表
		 * @return
		 */
		public List<Get> getReadList() {
			return m_gets;
		}
		/**
		 * 
		 * 函数名：setReadList
		 * 功能描述：设置批量读的列表
		 * @param gets
		 */
		public void setReadList(List<Get> gets) {
			this.m_gets = gets;
		}
		/**
		 * 
		 * 函数名：getOperationList
		 * 功能描述：返回实际写操作的列表
		 * @return
		 */
		public List<Row> getOperationList(){
			List<Row> list = new ArrayList<Row>();
			if(m_puts != null)
				list.add(m_puts);
			if(m_incr != null)
				list.add(m_incr);
			return list;
		}
		/**
		 * 
		 * 函数名：addPut
		 * 功能描述：	添加写操作
		 * @param family
		 * @param column
		 * @param value
		 */
		public void addPut(String family,String column, String value){
			if(family == null || m_key == null)
				return;
			if(m_timeStamp < 0)
				m_timeStamp = System.currentTimeMillis();
			if(m_puts == null)
				m_puts = new Put(m_key.getBytes(),m_timeStamp);
			m_puts.add(family.getBytes(), column.getBytes(),value.getBytes());
		}
		/**
		 * 
		 * 函数名：addInc
		 * 功能描述：	添加计数器操作
		 * @param family
		 * @param column
		 * @param value
		 */
		public void addInc(String family,String column, Long value){
			if(family == null || m_key == null)
				return;
			if(m_incr == null)
				m_incr = new Increment(m_key.getBytes());
			m_incr.addColumn(family.getBytes(), column.getBytes(), value);
		}

		
		public HbaseParam() {
			this(-1);
		}

		public HbaseParam(long timestamp) {
			m_qualityValue = new HashMap<String, String>();
			m_key = null;
			m_puts = null;
			m_incr = null;
			m_filters = new FilterList();
			m_timeStamp = timestamp;
			m_scanStartTime = -1;
			m_scanEndTime = -1;
			m_maxVersion = 0;
		}
		/**
		 * 
		 * 函数名：setTimeRange
		 * 功能描述： 设置扫描操作的起始结束时间
		 * @param start
		 * @param end
		 */
		public void setTimeRange(long start,long end){
			m_scanStartTime = start;
			m_scanEndTime = end;
		}
		/**
		 * 
		 * 函数名：setRowkey
		 * 功能描述： 写操作时,指定写如的行健
		 * @param rowkey
		 */
		public void setRowkey(String rowkey/* could be regex */) {
			m_key = rowkey;
		}
		/**
		 * 
		 * 函数名：removeValue
		 * 功能描述：移除指定的列族-列:值
		 * @param key
		 */
		public void removeValue(String key){
			m_qualityValue.remove(key);
		}

		/**
		 * 
		 * 函数名：setColumns
		 * 功能描述：设置扫描读取时,取的列
		 * @param columns
		 */
		public void setColumns(String[] columns) {
			for (String column : Arrays.asList(columns)) {
				setValue(column, null);
			}
		}
		/**
		 * 
		 * 函数名：setColumns
		 * 功能描述：设置扫描读取时,取的列
		 * @param columns
		 */
		public void setColumns(List<String> columns) {
			for (String column : columns) {
				setValue(column, null);
			}
		}
		/**
		 * 
		 * 函数名：setValue
		 * 功能描述：设置写入m_key行的列:值
		 * @param column
		 * @param value
		 */
		public void setValue(String column, String value) {
			if (column != null)
				m_qualityValue.put(column, value);
		}
		/**
		 * 
		 * 函数名：setMaxVersions
		 * 功能描述：设置读取时,返回的最大版本书
		 * @param versions
		 */
		public void setMaxVersions(Integer versions){
			m_maxVersion = versions;
		}
		
		public Integer getMaxVersions(){
			return m_maxVersion;
		}
		/**
		 * 
		 * 函数名：getRowKey
		 * 功能描述： 获取写入对象,或者读取对象的行健字节值
		 * @return
		 */
		public byte[] getRowKey() {
			if (m_key != null)
				return m_key.getBytes();
			else
				return null;
		}

		public Set<String> getColumns() {
			return m_qualityValue.keySet();
		}
		/**
		 * 
		 * 函数名：getColumnVal
		 * 功能描述：获取列族-列:值,这个对象既用于读也用于写,用于读取时其value值通常为空
		 * @return
		 */
		public Map<String, String> getColumnVal() {
			return m_qualityValue;
		}

		public void addFilter(Filter filter) {
			if (null != filter)
				m_filters.addFilter(filter);
		}
		/**
		 * 
		 * 函数名：addFilters
		 * 功能描述：设置读取时的过滤器
		 * @param filters
		 */
		public void addFilters(FilterList filters) {
			m_filters = filters;
		}
		/**
		 * 
		 * 函数名：getFilter
		 * 功能描述：获取过滤器列表
		 * @return
		 */
		public FilterList getFilter() {
			return m_filters;
		}
		/**
		 * 
		 * 函数名：setFilterType
		 * 功能描述：设置读取时过滤器的操作行为--全部通过,或者只需通过一个
		 * @param op
		 */
		public void setFilterType(Operator op){
			if(m_filters.getOperator() == op){
				return;
			}else{
				//copy the filter
				m_filters = new FilterList(op, m_filters.getFilters());
			}
		}
	}

	// for test
	// public static void main(String [] args){
	// //CRUD test
	// HbaseCRUD crud = new HbaseCRUD("IP_FILTER");
	// HbaseParam hparam = new HbaseParam();
	// HbaseResult hresult = new HbaseResult();
	// //set the selected columns
	// hparam.setColumns(BlobBolt.IP_PARAMATE);
	// //read from hbase table
	// String lastItem = crud.readhbase(hparam, hresult);
	// //parse result,and continue read data
	// do{
	// for(String string : hresult.getRowKeys())
	// System.out.println(string);
	// }while((lastItem = crud.readFrom(hparam, hresult, lastItem)) != null) ;
	// }
}
