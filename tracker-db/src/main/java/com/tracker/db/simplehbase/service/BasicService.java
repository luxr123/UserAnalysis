package com.tracker.db.simplehbase.service;

import java.util.List;

import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.simplehbase.request.PutRequest;
import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOWithKeyResult;

/**
 * BasicService
 * 
 * <pre>
 * Provides basic services.
 * </pre>
 * 
 * */
public interface BasicService {
	 public <T> List<T> findObjectList(Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo);
	  public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectAndKeyList(Class<? extends T> type, @Nullable QueryExtInfo queryExtInfo);
    /**
     * Find object with single row key.
     * */
    public <T> T findObject(String rowKey, Class<? extends T> type, QueryExtInfo queryExtInfo);

    /**
     * Find object and row key with single row key.
     * */
    public <T> SimpleHbaseDOWithKeyResult<T> findObjectAndKey(String rowKey, Class<? extends T> type, QueryExtInfo queryExtInfo);
    
    //=============================================================================================================================
    /**
     * Find POJO list with row list.
     * */
    public <T> List<T> findObjectList(List<String> rowKeyList, Class<? extends T> type, QueryExtInfo queryExtInfo);
    /**
     * Find object and row key with row list.
     * */
    public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectListAndKey(List<String> rowKeyList, Class<? extends T> type, QueryExtInfo queryExtInfo);
   
    //=============================================================================================================================
    /**
     * Find POJO list with range in [startString,endString).
     * */
    public <T> List<T> findObjectList(String startRowKey, String endRowKey, Class<? extends T> type, QueryExtInfo queryExtInfo);
    /**
     * Find POJO and row key list with range in [startString,endString).
     * */
    public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectAndKeyList(
            String startRowKey, String endRowKey, Class<? extends T> type, QueryExtInfo queryExtInfo);
    
    //=============================================================================================================================
    public <T> List<T> findObjectListByRowPrefix(String rowKeyPrefix, Class<? extends T> type,  QueryExtInfo queryExtInfo);

    public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectListAndKeyByRowPrefix(
    		String rowKeyPrefix, Class<? extends T> type,  QueryExtInfo queryExtInfo);

    public <T> List<T> findObjectListByRowPrefixList(List<String> rowKeyPrefixList, Class<? extends T> type,  QueryExtInfo queryExtInfo);

    public <T> List<SimpleHbaseDOWithKeyResult<T>> findObjectListAndKeyByRowPrefixList(
    		List<String> rowKeyPrefixList, Class<? extends T> type,  QueryExtInfo queryExtInfo);
    
    //=============================================================================================================================
    /**
     * Put POJO.
     * */
    public <T> void putObject(String rowKey, T t);

    /**
     * Put POJO list.
     * */
    public <T> void putObjectList(List<PutRequest<T>> putRequestList);
   
    //=============================================================================================================================
    /**
     * Delete POJO.
     * */
    
    public void deleteObject(String rowKey);

    /**
     * Delete POJO list.
     * */
    public void deleteObjectList(List<String> rowKeyList);
    
    /**
     * Batch delete POJO list.
     * */
    public void deleteObjectList(String startRowKey, String endRowKey);
    
    /**
     * Batch delete by row prefix
     * @param rowPrefix
     */
    public void deleteObjectByRowPrefix(String rowPrefix);
}
