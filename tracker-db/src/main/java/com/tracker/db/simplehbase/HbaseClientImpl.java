package com.tracker.db.simplehbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.simplehbase.request.DeleteRequest;
import com.tracker.db.simplehbase.request.PutRequest;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.util.Util;

/**
 * SimpleHbaseClient default implementation.
 * 
 * */
public class HbaseClientImpl extends HbaseClient {

    public HbaseClientImpl(HConnection hbaseConnection, Class<?>  type) {
		super(hbaseConnection, type);
	}
    
    public HbaseClientImpl(HConnection hbaseConnection, String tableName) {
		super(hbaseConnection, tableName);
	}

    public <T> List<T> findObjectList(Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
        return unwrap(findObjectListAndKeyByRowPrefix(null, type, queryExtInfo));
    }
    
    public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectAndKeyList(Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
        return findObjectListAndKeyByRowPrefix(null, type, queryExtInfo);
    }
    
    @Override
    public <T> T findObject(String rowKey, Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
        return unwrap(findObjectAndKey(rowKey, type, queryExtInfo));
    }
    
    public <T> List<T> findObjectList(List<String> rowKeyList, Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo){
    	return unwrap(findObjectListAndKey(rowKeyList, type, queryExtInfo));
    }
    
    @Override
    public <T> SimpleHbaseDOWithKeyResult<T> findObjectAndKey(String rowKey, Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
    	List<String> rowKeyList = new ArrayList<String>();
    	rowKeyList.add(rowKey);
    	List<SimpleHbaseDOWithKeyResult<T>> resultList = findObjectListAndKey(rowKeyList, type, queryExtInfo);
    	if(resultList != null && resultList.size() > 0)
    		return resultList.get(0);
    	return null;
    }

    @Override
	public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectListAndKey(List<String> rowKeyList, Class<? extends T> type, QueryExtInfo queryExtInfo) {
		Util.checkRowKey(rowKeyList);
        Util.checkNull(type);

    	//only query 1 version.
        if (queryExtInfo != null) {
            queryExtInfo.setMaxVersions(1);
        }
        
        List<SimpleHbaseDOWithKeyResult<T>> resultList = new ArrayList<SimpleHbaseDOWithKeyResult<T>>();
        HTableInterface htableInterface = htableInterface();
        try {
        	List<Get> gets = new ArrayList<Get>();
        	for(String rowKey: rowKeyList){
        	    Get get = constructGet(rowKey, queryExtInfo, type);
        	    //添加filter
                FilterList filterList = new FilterList();
                applyFilter(type, filterList, queryExtInfo);
                if(filterList.getFilters().size() > 0)
                	get.setFilter(filterList);
	            gets.add(get);
        	}
        	Result[] results = htableInterface.get(gets);
        	if(results != null){
        		for(Result result: results){
        			 SimpleHbaseDOWithKeyResult<T> t = convertToSimpleHbaseDOWithKeyResult(result, type);
                     if (t != null) {
                         resultList.add(t);
                     }
        		}
        	}
        } catch (IOException e) {
            throw new SimpleHBaseException("findObjectAndKey_internal. rowKeyList="
                    + rowKeyList + " type=" + type, e);
        } finally {
            Util.close(htableInterface);
        }
        return resultList;
	}
    
    @Override
    public <T> List<T> findObjectList(String startRowKey, String endRowKey,
            Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
        return unwrap(findObjectAndKeyList(startRowKey, endRowKey, type, queryExtInfo));
    }
    
    @Override
    public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectAndKeyList(
    		String startRowKey, String endRowKey, Class<? extends T> type, QueryExtInfo queryExtInfo) {
        Util.checkRowKey(startRowKey);
        Util.checkRowKey(endRowKey);
        Util.checkNull(type);

        //构造Scan
        Scan scan = constructScan(queryExtInfo, type);
        scan.setStartRow(Bytes.toBytes(startRowKey));
        scan.setStopRow(Bytes.toBytes(endRowKey));
        //构造filter
        FilterList filterList = new FilterList();
        applyFilter(type, filterList, queryExtInfo);
        if(filterList.getFilters().size() > 0)
        	scan.setFilter(filterList);
        return findObjectAndKeyList_internal(scan, type, queryExtInfo);
    }
    
	@Override
	public <T> List<T> findObjectListByRowPrefix(String rowKeyPrefix,
			Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
		List<String> rowPrefixList = null;
		if(rowKeyPrefix != null){
			rowPrefixList = new ArrayList<String>();
			rowPrefixList.add(rowKeyPrefix);
		}
		return  findObjectListByRowPrefixList(rowPrefixList, type, queryExtInfo);
	}
	
	@Override
	public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectListAndKeyByRowPrefix(String rowKeyPrefix,
			Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo) {
		List<String> rowPrefixList = null;
		if(rowKeyPrefix != null){
			rowPrefixList = new ArrayList<String>();
			rowPrefixList.add(rowKeyPrefix);
		}
		return findObjectListAndKeyByRowPrefixList(rowPrefixList, type, queryExtInfo);
	}
	

    @Override
	public <T> List<T> findObjectListByRowPrefixList(List<String> rowKeyPrefixList,
			Class<? extends T> type, QueryExtInfo queryExtInfo) {
    	return unwrap(findObjectListAndKeyByRowPrefixList(rowKeyPrefixList, type, queryExtInfo));
	}

	@Override
	public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectListAndKeyByRowPrefixList(
			List<String> rowKeyPrefixList, Class<? extends T> type,
			QueryExtInfo queryExtInfo) {
		Util.checkNull(type);
		  //构造Scan
		Scan scan = constructScan(queryExtInfo, type);
		//构造filter, 默认为and		
		FilterList filterList = new FilterList();
		if(rowKeyPrefixList != null && rowKeyPrefixList.size() > 0){
			//or filter
			FilterList rowFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String rowPrefix: rowKeyPrefixList){
				rowFilterList.addFilter(new PrefixFilter(Bytes.toBytes(rowPrefix)));
			}
			filterList.addFilter(rowFilterList);
		}
		applyFilter(type, filterList, queryExtInfo);
		if(filterList.getFilters().size() > 0)
			scan.setFilter(filterList);
		return findObjectAndKeyList_internal(scan, type, queryExtInfo);
	}

	private <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectAndKeyList_internal(Scan scan, Class<? extends T> type, QueryExtInfo queryExtInfo) {
    	long startIndex = 0L;
        long length = Long.MAX_VALUE;
        if (queryExtInfo != null && queryExtInfo.isLimitSet()) {
            startIndex = queryExtInfo.getStartIndex();
            length = queryExtInfo.getLength();
        }

        List<SimpleHbaseDOWithKeyResult<T>> resultList = new ArrayList<SimpleHbaseDOWithKeyResult<T>>();
        HTableInterface htableInterface = htableInterface();
        ResultScanner resultScanner = null;
        try {
            resultScanner = htableInterface.getScanner(scan);
            long ignoreCounter = startIndex;
            long resultCounter = 0L;
            Result result = null;
            while ((result = resultScanner.next()) != null) {
                if (ignoreCounter-- > 0) {
                    continue;
                }
                SimpleHbaseDOWithKeyResult<T> t = convertToSimpleHbaseDOWithKeyResult(result, type);
                if (t != null) {
                    resultList.add(t);
                    if (++resultCounter >= length) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new SimpleHBaseException("findObjectAndKeyList_internal.type=" + type, e);
        } finally {
            Util.close(resultScanner);
            Util.close(htableInterface);
        }

        return resultList;
    }

    @Override
	public <T> void putObjectMV(String rowKey, T t, long timestamp) {
    	 List<PutRequest<T>> putRequestList = new ArrayList<PutRequest<T>>();
         putRequestList.add(new PutRequest<T>(rowKey, t, timestamp));
         putObjectList(putRequestList);
	}

	@Override
    public <T> void putObject(String rowKey, T t) {
        List<PutRequest<T>> putRequestList = new ArrayList<PutRequest<T>>();
        putRequestList.add(new PutRequest<T>(rowKey, t));
        putObjectList(putRequestList);
    }

    @Override
    public <T> void putObjectList(List<PutRequest<T>> putRequestList) {
        putObjectList_internal(putRequestList);
    }

    private <T> void putObjectList_internal(List<PutRequest<T>> putRequestList) {
        Util.checkNull(putRequestList);
        if (putRequestList.isEmpty()) {
            return;
        }
        for (PutRequest<T> putRequest : putRequestList) {
            Util.checkPutRequest(putRequest);
        }

        TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(putRequestList.get(0).getT().getClass());
        List<Row> actions = new ArrayList<Row>();
        for (PutRequest<T> putRequest : putRequestList) {
        	byte[] row = Bytes.toBytes(putRequest.getRowKey());
            Put put = null;
            Increment increment = null;
            for (ColumnInfo columnInfo : typeInfo.findAllColumnInfo()) {
                if(columnInfo.isIncrementType){
                	Long value = convertPOJOFieldToLong(putRequest.getT(),columnInfo);
                	if(value == null)
                		continue;
                	if(increment == null){
                		increment = new Increment(row);
                	}
                	
                	increment.addColumn(columnInfo.familyBytes, columnInfo.qualifierBytes, value);
                } else {
                    byte[] value = convertPOJOFieldToBytes(putRequest.getT(),columnInfo);
                    if(value == null)
                    	continue;
                    if(put == null){
                		put = new Put(row);
                	}
                    if (putRequest.getTimestamp() == null) {
                        put.add(columnInfo.familyBytes, columnInfo.qualifierBytes, value);
                    } else {
                        put.add(columnInfo.familyBytes, columnInfo.qualifierBytes, putRequest.getTimestamp().longValue(), value);
                    }
                }
            }
            if(put != null)
            	actions.add(put);
            if(increment != null)
            	actions.add(increment);
        }

        HTableInterface htableInterface = htableInterface();
        try {
        	htableInterface.batch(actions);
        } catch (IOException e) {
            throw new SimpleHBaseException("putObjectList_internal. putRequestList=" + putRequestList, e);
        } catch (InterruptedException e) {
        	 throw new SimpleHBaseException("putObjectList_internal. putRequestList=" + putRequestList, e);
		} finally {
            Util.close(htableInterface);
        }
    }


    @Override
    public void deleteObject(String rowKey) {
        List<String> rowKeyList = new ArrayList<String>();
        rowKeyList.add(rowKey);
        deleteObjectList(rowKeyList);
    }

    @Override
    public void deleteObjectList(List<String> rowKeyList) {
        Util.checkNull(rowKeyList);

        List<DeleteRequest> deleteRequestList = new ArrayList<DeleteRequest>();
        for (String rowKey : rowKeyList) {
            deleteRequestList.add(new DeleteRequest(rowKey));
        }

        deleteObjectList_internal(deleteRequestList);
    }

    private void deleteObjectList_internal(
            List<DeleteRequest> deleteRequestList) {
        Util.checkNull(deleteRequestList);

        if (deleteRequestList.isEmpty()) {
            return;
        }

        for (DeleteRequest deleteRequest : deleteRequestList) {
            Util.checkDeleteRequest(deleteRequest);
        }

        List<Delete> deletes = new LinkedList<Delete>();
        for (DeleteRequest deleteRequest : deleteRequestList) {
            Delete delete = new Delete(Bytes.toBytes(deleteRequest.getRowKey()));
            deletes.add(delete);
        }
        HTableInterface htableInterface = htableInterface();
        try {
            htableInterface.delete(deletes);
        } catch (IOException e) {
            throw new SimpleHBaseException("deleteObjectList_internal. deleteRequestList = " + deleteRequestList, e);
        } finally {
            Util.close(htableInterface);
        }

        //successful delete will clear the items of deletes list.
        if (deletes.size() > 0) {
            throw new SimpleHBaseException(
                    "deleteObjectList_internal. deletes=" + deletes);
        }
    }

    @Override
    public void deleteObjectList(String startRowKey, String endRowKey) {
        Util.checkRowKey(startRowKey);
        Util.checkRowKey(endRowKey);
        delete_internal_with_scan_first(startRowKey, endRowKey, null);
    }

    /**
     * columnInfoList and hbaseColumnSchemaList can not be null or empty both.
     * */
    private void delete_internal_with_scan_first(String startRowKey,
    		String endRowKey, @Nullable Filter filter) {
        final int deleteBatch = getDeleteBatch();
        while (true) {
        	String nextStartRowkey = startRowKey;
            Scan temScan = new Scan();
            temScan.setStartRow(Bytes.toBytes(nextStartRowkey));
            temScan.setStopRow(Bytes.toBytes(endRowKey));
            temScan.setFilter(filter);

            List<Delete> deletes = new LinkedList<Delete>();

            HTableInterface htableInterface = htableInterface();
            ResultScanner resultScanner = null;
            try {
                resultScanner = htableInterface.getScanner(temScan);
                Result result = null;
                while ((result = resultScanner.next()) != null) {
                    Delete delete = new Delete(result.getRow());
                    nextStartRowkey = Bytes.toString(result.getRow());
                    deletes.add(delete);
                    if (deletes.size() >= deleteBatch) {
                        break;
                    }
                }
            } catch (IOException e) {
                throw new SimpleHBaseException("delete_internal. scan = "
                        + temScan, e);
            } finally {
                Util.close(resultScanner);
                Util.close(htableInterface);
            }

            if (deletes.size() == 0) {
                return;
            }

            try {
                htableInterface = htableInterface();
                htableInterface.delete(deletes);
            } catch (IOException e) {
                throw new SimpleHBaseException("delete_internal. scan = " + temScan, e);
            } finally {
                Util.close(htableInterface);
            }

            //successful delete will clear the items of deletes list.
            if (deletes.size() > 0) {
                throw new SimpleHBaseException("delete_internal fail. deletes=" + deletes);
            }

            if (deletes.size() < deleteBatch) {
                return;
            }
        }
    }
    
    public void deleteObjectByRowPrefix(String rowPrefix){
    	 final int deleteBatch = getDeleteBatch();
    	 Scan temScan = new Scan();
         temScan.setFilter(new PrefixFilter(Bytes.toBytes(rowPrefix)));
         HTableInterface htableInterface = htableInterface();
         ResultScanner resultScanner = null;
         try {
			resultScanner = htableInterface.getScanner(temScan);
             List<Delete> deletes = new LinkedList<Delete>();
             Result result = null;
             while ((result = resultScanner.next()) != null) {
                 Delete delete = new Delete(result.getRow());
                 deletes.add(delete);
                 if (deletes.size() >= deleteBatch) {
                	 htableInterface.delete(deletes);
                	 deletes = new LinkedList<Delete>();
                 }
             }
             if (deletes.size() > 0) {
            	 htableInterface.delete(deletes);
             }
         } catch (IOException e) {
			 throw new SimpleHBaseException("delete_internal. scan = " + temScan, e);
         } finally {
            Util.close(resultScanner);
            Util.close(htableInterface);
         }
    }

	@Override
	public long count(String startRow, String endRow) {
		Scan scan = new Scan();
		scan.setCaching(getScanCaching());
	    scan.setStartRow(Bytes.toBytes(startRow));
	    scan.setStopRow(Bytes.toBytes(endRow));
	    FilterList filterList = new FilterList();
	    filterList.addFilter(new KeyOnlyFilter());
	    scan.setFilter(filterList);
	    return count(scan);
	}

	@Override
	public long countByRowPrefix(String rowPrefix) {
		Util.checkNull(rowPrefix);
		 //构造Scan
		Scan scan = new Scan();
	    scan.setCaching(getScanCaching());
	    FilterList filterList = new FilterList();
	    filterList.addFilter(new KeyOnlyFilter());
	    filterList.addFilter(new PrefixFilter(Bytes.toBytes(rowPrefix)));
	    scan.setFilter(filterList);
		return count(scan);
	}
	
	@Override
	public <T> long countByRowPrefix(String rowPrefix, Class<? extends T> type,
			QueryExtInfo queryExtInfo) {
		Scan scan = constructScan(queryExtInfo, type);
		//构造filter, 默认为and		
		FilterList filterList = new FilterList();
		filterList.addFilter(new PrefixFilter(Bytes.toBytes(rowPrefix)));
		applyFilter(type, filterList, queryExtInfo);
		scan.setFilter(filterList);
		return count(scan);
	}

	private long count(Scan scan){
		long resultCounter = 0L;
		HTableInterface htableInterface = htableInterface();
	    ResultScanner resultScanner = null;
	    try {
	    	resultScanner = htableInterface.getScanner(scan);
	        while (resultScanner.next() != null) {
	        	resultCounter++;
	        }
	    } catch (IOException e) {
	        throw new SimpleHBaseException("countByRowPrefix=", e);
	    } finally {
	        Util.close(resultScanner);
	        Util.close(htableInterface);
	    }
		return resultCounter;
	}
}
