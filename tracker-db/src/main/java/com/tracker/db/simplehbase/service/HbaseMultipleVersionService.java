package com.tracker.db.simplehbase.service;

import java.util.List;

import com.tracker.db.simplehbase.request.QueryExtInfo;
import com.tracker.db.simplehbase.result.SimpleHbaseDOResult;

/**
 * HbaseMultipleVersionService
 * 
 * <pre>
 * Provides hbase multiple version related service.
 * </pre>
 * 
 * @author jason.hua
 */
public interface HbaseMultipleVersionService {

    /**
     * Put POJO with timestamp.
     * 
     * @param rowKey rowKey.
     * @param t POJO.
     * @param timestamp timestamp.
     * */
    public <T> void putObjectMV(String rowKey, T t, long timestamp);

    /**
     * Find object with row key.
     * 
     * @param rowKey rowKey.
     * @param type POJO type.
     * @param queryExtInfo queryExtInfo.
     * @return SimpleHbaseDOResult list, timestamp desc ordered.
     * */
//    public <T> List<SimpleHbaseDOResult<T>> findObjectMV(String rowKey, Class<? extends T> type, QueryExtInfo queryExtInfo);
}
