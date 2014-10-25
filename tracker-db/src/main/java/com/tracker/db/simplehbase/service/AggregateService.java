package com.tracker.db.simplehbase.service;

import com.tracker.db.simplehbase.request.QueryExtInfo;

/**
 * AggregateService.
 * 
 * <pre>
 * Provides aggregate function on hbase table.
 * 
 * @author jason.hua
 *
 */
public interface AggregateService {
    /**
     * Count POJO with range in [startString,endString).
     * 
     * @param startString startString.
     * @param endString endString.
     * 
     * @return count result.
     * */
    public long count(String startRow, String endRow);

    public long countByRowPrefix(String rowPrefix);
    
    public <T> long countByRowPrefix(String rowPrefix, Class<? extends T> type, QueryExtInfo queryExtInfo);
}
