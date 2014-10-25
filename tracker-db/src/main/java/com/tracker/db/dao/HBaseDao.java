package com.tracker.db.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;
import com.tracker.coprocessor.generated.TopProtos.TopRequest;
import com.tracker.coprocessor.generated.TopProtos.TopRequest.Builder;
import com.tracker.coprocessor.generated.TopProtos.TopResponse;
import com.tracker.coprocessor.generated.TopProtos.TopService;
import com.tracker.db.simplehbase.HbaseClientImpl;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.Util;

public class HBaseDao extends HbaseClientImpl{
	public <T> HBaseDao(HConnection hbaseConnection, Class<T> type){
		super(hbaseConnection, type);
	}
	
	public <T> HBaseDao(HConnection hbaseConnection, String tableName){
		super(hbaseConnection, tableName);
	}
	
	public <T>  Map<String, T> unWrapToMap(List<SimpleHbaseDOWithKeyResult<T>> list){
		Map<String, T> resultMap = new HashMap<String, T>();
		if(list != null){
			for(SimpleHbaseDOWithKeyResult<T> result: list){
				resultMap.put(result.getRowKey(), result.getT());
			}
		}
		return resultMap;
	}
	
	public long incrementColumnValue(String row, byte[] family, byte[] qualifier, long value){
		long result = 0;
		HTableInterface htable = null;
		try {
			htable = htableInterface();
			result = htable.incrementColumnValue(Bytes.toBytes(row), family, qualifier, value);
		} catch (IOException e) {
			throw new SimpleHBaseException("can not increment row:" + row + ", family: " + Bytes.toString(qualifier), e);
		} finally{
			Util.close(htable);
		}
		return result;
	}
	
	public void batchIncValues(byte[] family, byte[] qualifier, Map<String, Long> rowValue){
		if(rowValue == null || rowValue.size()  == 0)
			return;
        List<Row> actions = new ArrayList<Row>();
		for(String row: rowValue.keySet()){
			Increment inc = new Increment(row.getBytes());
			inc.addColumn(family, qualifier, rowValue.get(row));
			actions.add(inc);
		}
		batch(actions);
	}
	
	public void putRow(String row, byte[] family, byte[] qualifier, byte[] value){
		HTableInterface htable = null;
		try {
			Put put = new Put(Bytes.toBytes(row));
			put.add(family, qualifier, value);
			htable = htableInterface();
			htable.put(put);
		} catch (IOException e) {
			throw new SimpleHBaseException("can not put data to hbase", e);
		} finally{
			Util.close(htable);
		}
	}
	
	public void putRows(List<Put> puts){
		if(puts == null || puts.size() == 0)
			return;
		HTableInterface htable = null;
		try {
			htable = htableInterface();
			htable.put(puts);
		} catch (IOException e) {
			throw new SimpleHBaseException("can not put data to hbase", e);
		} finally{
			Util.close(htable);
		}
	}
	
	public void batch(List<? extends Row> actions){
		if(actions == null || actions.size()  == 0)
			return;
		HTableInterface htable = null;
		try {
			htable = htableInterface();
			htable.batch(actions);
		} catch (IOException e) {
			throw new SimpleHBaseException("can not put data to hbase", e);
		} catch (InterruptedException e) {
			throw new SimpleHBaseException("can not put data to hbase", e);
		} finally{
			Util.close(htable);
		}
	}
	
	public HTableInterface getTable(){
		return htableInterface();
	}
	
	public Map<byte[], TopResponse> GetTop(final Scan scan,final int topN) throws ServiceException, Throwable{
		HTableInterface htable = null;
		htable = htableInterface();
		Map<byte[], TopResponse> results=htable.coprocessorService(TopService.class, null, null, 
				new Batch.Call<TopService, TopResponse>() {
				public TopResponse call(TopService top) throws IOException{
					Builder builder=TopRequest.newBuilder();
					builder.setScan(ProtobufUtil.toScan(scan));
					builder.setTopN(topN);
					
					ServerRpcController controller=new ServerRpcController();
					BlockingRpcCallback<TopResponse> rpcCallback = new BlockingRpcCallback<TopResponse>();
					top.getTop(controller, builder.build(), rpcCallback);
					
					TopResponse response=rpcCallback.get();
					if (controller.failedOnException()) {
						throw controller.getFailedOn();
					}
					return response;
				}
			});
		return results;
	}
	
	public void deleteRows(List<String> rows){
		List<Delete> deletes = new ArrayList<Delete>();
		for(String row: rows){
			Delete delete = new Delete(Bytes.toBytes(row));
			deletes.add(delete);
		}

		HTableInterface htableInterface = htableInterface();
		try {
			htableInterface.delete(deletes);
		} catch (IOException e) {
			throw new SimpleHBaseException("error to deleteRows, rows:" + rows, e);
		} finally {
			Util.close(htableInterface);
		}
	}
}
