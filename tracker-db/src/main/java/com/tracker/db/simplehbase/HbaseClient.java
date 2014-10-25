package com.tracker.db.simplehbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseCellResult;
import com.tracker.db.simplehbase.result.SimpleHbaseDOResult;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;
import com.tracker.db.simplehbase.service.AggregateService;
import com.tracker.db.simplehbase.service.BasicService;
import com.tracker.db.simplehbase.service.HbaseMultipleVersionService;
import com.tracker.db.simplehbase.type.TypeHandler;
import com.tracker.db.util.Util;


/**
 * SimpleHbaseClient's skeleton implementation.
 * 
 * @author jason
 */
abstract public class HbaseClient implements BasicService,AggregateService,HbaseMultipleVersionService {
	
    private int scanCachingSize = 100; // scan caching size.
    private int deleteBatchSize = 300; // delete batch size
    
    private HConnection hbaseConnection;
    private String tableName;

    public HbaseClient(HConnection hbaseConnection, String tableName){
    	this.hbaseConnection = hbaseConnection;
    	this.tableName = tableName;
    }
    
    public HbaseClient(HConnection hbaseConnection, Class type){
    	this.hbaseConnection = hbaseConnection;
    	TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(type);
    	tableName = typeInfo.getTableName();
    }
    
    /**
     * Get HTableInterface.
     * */
    public HTableInterface htableInterface() {
        try {
			return hbaseConnection.getTable(tableName);
		} catch (IOException e) {
			throw new SimpleHBaseException("error to get HTableInterface for tableName :" + tableName);
		}
    }
    
    /**
     * Get scan's caching size.
     * */
    protected int getScanCaching() {
        return scanCachingSize;
    }

    /**
     * Get batch size when do delete.
     * */
    protected int getDeleteBatch() {
        return deleteBatchSize;
    }
    
    protected <T> T unwrap(SimpleHbaseDOWithKeyResult<T> simpleHbaseDOWithKeyResult) {
        if (simpleHbaseDOWithKeyResult == null) {
            return null;
        }
        return simpleHbaseDOWithKeyResult.getT();
    }
    
    protected <T> List<T> unwrap(
            List<SimpleHbaseDOWithKeyResult<T>> simpleHbaseDOWithKeyResultList) {
        List<T> resultList = new ArrayList<T>();
        if (!simpleHbaseDOWithKeyResultList.isEmpty()) {
            for (SimpleHbaseDOWithKeyResult<T> t : simpleHbaseDOWithKeyResultList) {
                resultList.add(unwrap(t));
            }
        }
        return resultList;
    }
    
    
    
    /**
     * Construct Scan.
     * */
    protected <T> Scan constructScan(@Nullable QueryExtInfo<T> queryExtInfo, Class<? extends T> type) {
        Scan scan = new Scan();
    	scan.setCaching(getScanCaching());
        
        //解析QueryExtInfo
        if (queryExtInfo != null) {
            if (queryExtInfo.isMaxVersionSet()) {
                scan.setMaxVersions(queryExtInfo.getMaxVersions());
            }
            if (queryExtInfo.isTimeRangeSet()) {
                try {
                    scan.setTimeRange(queryExtInfo.getMinStamp(), queryExtInfo.getMaxStamp());
                } catch (IOException e) {
                    throw new SimpleHBaseException("should never happen.", e);
                }
            }
        }
        //添加需要获取的column
        applyRequestFamilyAndQualifier(getColumnInfoList(queryExtInfo, type), scan);
        return scan;
    }

    /**
     * Construct Get.
     * */
    protected <T> Get constructGet(String rowKey, @Nullable QueryExtInfo<T> queryExtInfo, Class<? extends T> type) {
    	Util.checkRowKey(rowKey);
        Get get = new Get(Bytes.toBytes(rowKey));
        //解析QueryExtInfo
        if (queryExtInfo != null) {
            if (queryExtInfo.isMaxVersionSet()) {
            	try {
					get.setMaxVersions(queryExtInfo.getMaxVersions());
				} catch (IOException e) {
					 throw new SimpleHBaseException("should never happen.", e);
				}
            }
            if (queryExtInfo.isTimeRangeSet()) {
                try {
                	get.setTimeRange(queryExtInfo.getMinStamp(), queryExtInfo.getMaxStamp());
                } catch (IOException e) {
                    throw new SimpleHBaseException("should never happen.", e);
                }
            }
        }
        //添加需要获取的column
        applyRequestFamilyAndQualifier(getColumnInfoList(queryExtInfo, type), get);
        return get;
    }

    /**
     * Apply family and qualifier to scan request, to prevent return more data
     * than we need.
     * */
    protected <T> void applyRequestFamilyAndQualifier(List<ColumnInfo> columnInfoList, Scan scan) {
        for (ColumnInfo columnInfo : columnInfoList) {
            scan.addColumn(columnInfo.familyBytes, columnInfo.qualifierBytes);
        }
    }
    
    protected <T> void applyRequestFamilyAndQualifier(List<ColumnInfo> columnInfoList, Get get) {
        for (ColumnInfo columnInfo : columnInfoList) {
            get.addColumn(columnInfo.familyBytes, columnInfo.qualifierBytes);
        }
    }
    
    protected <T> List<ColumnInfo> getColumnInfoList(QueryExtInfo<T> queryExtInfo, Class<? extends T> type) {
  	  	TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(type);
    	 if(queryExtInfo == null || queryExtInfo.getColumnList() == null){
    		 return typeInfo.findAllColumnInfo();
    	 }
    	 
		 List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
		 for (String columnStr: queryExtInfo.getColumnList()) {
	        ColumnInfo columnInfo = typeInfo.findColumnInfo(columnStr);
	        columnInfoList.add(columnInfo);	
		 }      	
		 return columnInfoList;
    }
    
    protected <T> void applyFilter(Class<? extends T> type, FilterList filterList, QueryExtInfo queryExtInfo){
    	if(queryExtInfo == null)
    		return;
    	//必须存在的column
    	 List<ColumnInfo> existColumnList = queryExtInfo.getExistColumnList();
    	 if(existColumnList != null){
    		 for(ColumnInfo columnInfo: existColumnList){
 				SingleColumnValueFilter filter =new SingleColumnValueFilter(columnInfo.getFamilyBytes(),
 						columnInfo.getQualifierBytes(), CompareOp.NOT_EQUAL, Bytes.toBytes("")); 
 				filter.setFilterIfMissing(true);
 				filterList.addFilter(filter);
    		 }
    	 }
    	 //必须不存在的column
    	 List<ColumnInfo> notExistColumnList = queryExtInfo.getNotExistColumnList();
    	 if(notExistColumnList != null){
    		 for(ColumnInfo columnInfo: notExistColumnList){
 				SingleColumnValueFilter filter =new SingleColumnValueFilter(columnInfo.getFamilyBytes(),
 						columnInfo.getQualifierBytes(), CompareOp.EQUAL, Bytes.toBytes("")); 
 				filterList.addFilter(filter);
    		 }
    	 }
    	 //column value过滤
    	 Object obj = queryExtInfo.getObj();
    	 if(obj == null)
    		 return;
    	 TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(type);
         List<ColumnInfo> columnInfoList = typeInfo.findAllColumnInfo();
         for (ColumnInfo columnInfo : columnInfoList) {
        	 byte[] value = convertPOJOFieldToBytes(obj, columnInfo);
        	 if(value != null){
        		 SingleColumnValueFilter filter =new SingleColumnValueFilter(columnInfo.getFamilyBytes(),
  						columnInfo.getQualifierBytes(), CompareOp.EQUAL, value); 
        		 filterList.addFilter(filter);
        	 }
         }
    }
    
    /**
     * Convert hbase result to SimpleHbaseCellResult list.
     * 
     * */
    protected <T> List<SimpleHbaseCellResult> convertToSimpleHbaseCellResultList(
            Result hbaseResult, Class<? extends T> type) {
        List<Cell> cells = hbaseResult.listCells();
        if (cells == null || cells.size() == 0) {
            return new ArrayList<SimpleHbaseCellResult>();
        }
        try {
        	 TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(type);
            List<SimpleHbaseCellResult> resultList = new ArrayList<SimpleHbaseCellResult>();
            for (Cell cell : cells) {
                byte[] familyBytes = CellUtil.cloneFamily(cell);
                String familyStr = Bytes.toString(familyBytes);
                byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
                String qualifierStr = Bytes.toString(qualifierBytes);
                byte[] hbaseValue = CellUtil.cloneValue(cell);
                ColumnInfo columnInfo = typeInfo.findColumnInfo(familyStr, qualifierStr);
                TypeHandler typeHandler = columnInfo.getTypeHandler();
                Object valueObject = null;
                if(columnInfo.isStoreStringType){
                	valueObject = typeHandler.stringToObject(hbaseValue);
                } else {
                	valueObject = typeHandler.toObject(hbaseValue);
                }
                long ts = cell.getTimestamp();
                Date tsDate = new Date(ts);

                SimpleHbaseCellResult cellResult = new SimpleHbaseCellResult();

                cellResult.setFamilyStr(familyStr);
                cellResult.setQualifierStr(qualifierStr);
                cellResult.setValueObject(valueObject);
                cellResult.setTsDate(tsDate);

                resultList.add(cellResult);
            }

            byte[] row = CellUtil.cloneRow(cells.get(0));
            String rowKey = Bytes.toString(row);

            for (SimpleHbaseCellResult cell : resultList) {
                cell.setString(rowKey);
            }
            return resultList;
        } catch (Exception e) {
            throw new SimpleHBaseException("convert result exception. result=" + hbaseResult, e);
        }
    }

    /**
     * convert hhbase result to SimpleHbaseDOWithKeyResult.
     * 
     * @param hbaseResult hbase result.
     * @param type POJO type.
     * 
     * @return SimpleHbaseDOWithKeyResult.
     * */
    protected <T> SimpleHbaseDOWithKeyResult<T> convertToSimpleHbaseDOWithKeyResult(
            Result hbaseResult, Class<? extends T> type) {
    	 List<Cell> cells = hbaseResult.listCells();
         if (cells == null || cells.size() == 0) {
             return null;
         }
        try {
            TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(type);
            T result = type.newInstance();

            for (Cell cell : cells) {
                byte[] familyBytes = CellUtil.cloneFamily(cell);
                byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
                byte[] hbaseValue = CellUtil.cloneValue(cell);

                ColumnInfo columnInfo = typeInfo.findColumnInfo(Bytes.toString(familyBytes), Bytes.toString(qualifierBytes));
                TypeHandler typeHandler = columnInfo.getTypeHandler();
                Object value = null;
                if(columnInfo.isStoreStringType){
                	value = typeHandler.stringToObject(hbaseValue);
                } else {
                	value = typeHandler.toObject(hbaseValue);
                }
                if (value != null) {
                    columnInfo.field.set(result, value);
                }
            }

            byte[] row = CellUtil.cloneRow(cells.get(0));
            String rowKey = Bytes.toString(row);

            SimpleHbaseDOWithKeyResult<T> pojoWithKey = new SimpleHbaseDOWithKeyResult<T>();
            pojoWithKey.setString(rowKey);
            pojoWithKey.setT(result);
            return pojoWithKey;
        } catch (Exception e) {
            throw new SimpleHBaseException("convert result exception. result=" + hbaseResult + " type=" + type, e);
        }
    }

    /**
     * Convert hbase result to SimpleHbaseDOResult.
     * 
     * @param hbaseResult hbase result.
     * @param type POJO type.
     * 
     * @return SimpleHbaseDOResult list, timestamp desc ordered.
     * */
    protected <T> List<SimpleHbaseDOResult<T>> convertToSimpleHbaseDOResult(
            Result hbaseResult, Class<? extends T> type) {
    	 List<Cell> cells = hbaseResult.listCells();
         if (cells == null || cells.size() == 0) {
            return new ArrayList<SimpleHbaseDOResult<T>>();
        }

        TreeMap<Long, T> temMap = new TreeMap<Long, T>(Collections.reverseOrder());
        TypeInfo typeInfo = TypeInfoHolder.findTypeInfo(type);

        try {
        	 for (Cell cell : cells) {
                byte[] familyBytes = CellUtil.cloneFamily(cell);
                byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
                byte[] hbaseValue = CellUtil.cloneValue(cell);
                long ts = cell.getTimestamp();

                if (!temMap.containsKey(ts)) {
                    temMap.put(ts, type.newInstance());
                }

                ColumnInfo columnInfo = typeInfo.findColumnInfo(
                        Bytes.toString(familyBytes),
                        Bytes.toString(qualifierBytes));

                TypeHandler typeHandler = columnInfo.getTypeHandler();
                Object value = null;
                if(columnInfo.isStoreStringType){
                	value = typeHandler.stringToObject(hbaseValue);
                } else {
                	value = typeHandler.toObject(hbaseValue);
                }

                if (value != null) {
                    columnInfo.field.set(temMap.get(ts), value);
                }
            }

        	byte[] row = CellUtil.cloneRow(cells.get(0));
            String rowKey = Bytes.toString(row);

            List<SimpleHbaseDOResult<T>> result = new ArrayList<SimpleHbaseDOResult<T>>();

            for (Long ts : temMap.keySet()) {
                SimpleHbaseDOResult<T> r = new SimpleHbaseDOResult<T>();
                r.setString(rowKey);
                r.setTimestamp(ts);
                r.setT(temMap.get(ts));
                result.add(r);
            }
            return result;
        } catch (Exception e) {
            throw new SimpleHBaseException("convert result exception. result="
                    + hbaseResult + " type=" + type, e);
        }
    }

    /**
     * Convert t's field to bytes.
     * */
    protected <T> byte[] convertPOJOFieldToBytes(T t, ColumnInfo columnInfo) {
        try {
            Object value = columnInfo.field.get(t);
            if(value == null)
            	return null;
            return convertValueToBytes(value, columnInfo);
        } catch (Exception e) {
            throw new SimpleHBaseException(e);
        }
    }
    
    protected <T> Long convertPOJOFieldToLong(T t, ColumnInfo columnInfo) {
        try {
            Object value = columnInfo.field.get(t);
            if(value == null)
            	return null;
            return (Long) value;
        } catch (Exception e) {
            throw new SimpleHBaseException(e);
        }
    }

    /**
     * Convert value to bytes.
     * */
    protected byte[] convertValueToBytes(Object value, ColumnInfo columnInfo) {
    	if(value == null)
    		return null;
        TypeHandler typeHandler = columnInfo.getTypeHandler();
        if(columnInfo.isStoreStringType){
        	return typeHandler.stringToBytes(value);
        }
        return typeHandler.toBytes(value);
    }

}
