package com.tracker.db.dao.kpi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.tracker.coprocessor.generated.FilterRowCountProtos;
import com.tracker.coprocessor.generated.FilterRowCountProtos.CountRequest;
import com.tracker.coprocessor.generated.FilterRowCountProtos.CountResponse;
import com.tracker.coprocessor.generated.FilterRowCountProtos.RowCountService;
import com.tracker.db.dao.HBaseDao;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam.SearchRowGenerator;
import com.tracker.db.dao.kpi.entity.UnSummableKpiParam.WebSiteRowGenerator;

/**
 * 基于hbase的不可累加数据（IP,UV)实现
 * @author jason.hua
 *
 */
public class UnSummableKpiHBaseDaoImpl implements UnSummableKpiDao{
	public final static String UNSUMMABLE_KPI_DAY_TABLE = "rt_kpi_unsummable_day";
	public final static String UNSUMMABLE_KPI_WEEK_TABLE = "rt_kpi_unsummable_week";
	public final static String UNSUMMABLE_KPI_MONTH_TABLE = "rt_kpi_unsummable_month";
	public final static String UNSUMMABLE_KPI_YEAR_TABLE = "rt_kpi_unsummable_year";

	private HBaseDao unSummableKpiTable;

	private final static byte[] DEFAULT_CF = "data".getBytes(); 
	private final static byte[] EMPTY_VALUE = "".getBytes();
	
	public UnSummableKpiHBaseDaoImpl(HConnection hbaseConnection, String tableName) {
		unSummableKpiTable = new HBaseDao(hbaseConnection, tableName);
	}

	@Override
	public void updateUnSummableKpi(List<String> rowList){
		if(rowList == null || rowList.size() == 0)
			return;
		List<Put> puts = new ArrayList<Put>();
		for(String row: rowList){
			Put put = new Put(Bytes.toBytes(row));
			put.add(DEFAULT_CF, EMPTY_VALUE, EMPTY_VALUE);
			puts.add(put);
		}
		unSummableKpiTable.putRows(puts);
	}

	@Override
	public Map<String, Long> getWebSiteUnSummableKpi(String kpi, String date, String webId, String sign, Integer visitorType, List<String> fields){
		Map<String, Long> kpiMap= new HashMap<String, Long>();
		List<String> rowPrefixList = new ArrayList<String>();
		for(int i = 0; i < fields.size(); i++){
			rowPrefixList.add(WebSiteRowGenerator.generateRowPrefix(sign, kpi, date, webId, visitorType, fields.get(i)));
		}
		
		//获取UV或者IP数， Map<field, count>
		kpiMap = getKpi(rowPrefixList, UnSummableKpiParam.WebSiteRowGenerator.FIELD_INDEX);
		return kpiMap;
	}
	
	@Override
	public Long getSearchUnSummableKpiForDate(String kpi, String date, String webId, String sign, Integer seId, Integer searchType){
		//获取UV或者IP数
		String rowPrefix = SearchRowGenerator.generateRowPrefix(sign, kpi, date, webId, seId, searchType);
		Map<String, Long> kpiMap = getKpi(Lists.newArrayList(rowPrefix), UnSummableKpiParam.SearchRowGenerator.TIME_INDEX);
		if(kpiMap.size() > 0)
			return kpiMap.get(date);
		return 0L;
	}	

	private Map<String, Long> getKpi(List<String> rowPrefixList, int fieldIndex){
		Map<String, Long> result = new HashMap<String, Long>();
		try {
			//generate scan
			Scan scan = new Scan();
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String rowPrefix: rowPrefixList){
				filterList.addFilter(new PrefixFilter(Bytes.toBytes(rowPrefix)));
			}
//			filterList.addFilter(new KeyOnlyFilter());
			scan.setFilter(filterList);
			//build request
			final CountRequest request = CountRequest.newBuilder()
					.setScan(ProtobufUtil.toScan(scan))
					.setFieldIndex(fieldIndex)
					.build();
			//execute
			Map<byte[], List<FilterRowCountProtos.KV>> results = unSummableKpiTable.getTable().coprocessorService(RowCountService.class, null, null,
					new Batch.Call<RowCountService, List<FilterRowCountProtos.KV>>() {

						@Override
						public List<FilterRowCountProtos.KV> call(RowCountService instance) throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<CountResponse> rpccall = new BlockingRpcCallback<CountResponse>();
							instance.getRowCount(controller, request, rpccall);
							CountResponse response = rpccall.get();
							return response.getKvList();
						}
					});
			
			for (Entry<byte[], List<FilterRowCountProtos.KV>> entry : results.entrySet()) {
				List<FilterRowCountProtos.KV> list = entry.getValue();
				for(FilterRowCountProtos.KV k : list){
					if(result.containsKey(k.getKey())){
						result.put(k.getKey(), result.get(k.getKey()) + k.getValue());
					} else {
						result.put(k.getKey(), k.getValue());
					}
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return result;
	}
}

