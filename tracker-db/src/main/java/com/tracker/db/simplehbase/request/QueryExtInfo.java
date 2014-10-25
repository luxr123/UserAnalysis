package com.tracker.db.simplehbase.request;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.tracker.db.simplehbase.ColumnInfo;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.util.Util;

/**
 * QueryExtInfo
 * 
 * <pre>
 * Extra info when do query. So far, the following info can be supplied.
 * maxVersions
 * minTimeStamp and maxTimeStamp
 * startIndex and length when do scan, startIndex is 0-based.
 * </pre>
 * */
public class QueryExtInfo<T> {

    private boolean isMaxVersionSet;
    private int     maxVersions;

    private boolean isTimeRangeSet;
    private long    minStamp;
    private long    maxStamp;

    private boolean isLimitSet;
    private long    startIndex;
    private long    length;

    //过滤值
    private T obj;
    
    //该表中必须存在的column，否则该行记录被过滤
    private List<ColumnInfo> existColumnList = new ArrayList<ColumnInfo>();
    
    //该表中不能存在的column，否则该行记录被过滤
    private List<ColumnInfo> notExistColumnList = new ArrayList<ColumnInfo>();
    
    //需要获取的column
    private List<String> columnList = null;
    
    public QueryExtInfo() {
    }

    public void setMaxVersions(int maxVersions) {
        if (maxVersions < 1) {
            throw new SimpleHBaseException(
                    "maxVersions is smaller than 1. maxVersions=" + maxVersions);
        }
        this.maxVersions = maxVersions;
        this.isMaxVersionSet = true;
    }

    public void setTimeStamp(Date ts) {
        Util.checkNull(ts);
        setTimeStamp(ts.getTime());
    }

    public void setTimeStamp(long ts) {
        setTimeRange(ts, ts + 1);
    }

    public void setTimeRange(Date minStamp, Date maxStamp) {
        Util.checkNull(minStamp);
        Util.checkNull(maxStamp);
        setTimeRange(minStamp.getTime(), maxStamp.getTime());
    }

    public void setTimeRange(long minStamp, long maxStamp) {
        if (maxStamp < minStamp) {
            throw new SimpleHBaseException(
                    "maxStamp is smaller than minStamp. minStamp=" + minStamp
                            + " maxStamp=" + maxStamp);
        }
        this.minStamp = minStamp;
        this.maxStamp = maxStamp;
        this.isTimeRangeSet = true;
    }

    public void setLimit(long startIndex, long length) {
        if (startIndex < 0) {
            throw new SimpleHBaseException("startIndex is invalid. startIndex="
                    + startIndex);
        }
        if (length < 1) {
            throw new SimpleHBaseException("length is invalid. length="
                    + length);
        }
        this.startIndex = startIndex;
        this.length = length;
        this.isLimitSet = true;
    }
    
    public void addColumn(String qualifier){
    	if(columnList == null)
    		columnList = new ArrayList<String>();
    	columnList.add(qualifier);
    }

    public boolean isLimitSet() {
        return isLimitSet;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public long getLength() {
        return length;
    }

    public boolean isMaxVersionSet() {
        return isMaxVersionSet;
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public boolean isTimeRangeSet() {
        return isTimeRangeSet;
    }

    public long getMinStamp() {
        return minStamp;
    }

    public long getMaxStamp() {
        return maxStamp;
    }

	public T getObj() {
		return obj;
	}

	public void setObj(T obj) {
		this.obj = obj;
	}

	public List<ColumnInfo> getExistColumnList() {
		return existColumnList;
	}

	public void setExistColumnList(List<ColumnInfo> existColumnList) {
		this.existColumnList = existColumnList;
	}

	public void addExistColumn(ColumnInfo columnInfo){
		Util.checkNull(columnInfo);
		this.existColumnList.add(columnInfo);
	}
	
	public List<ColumnInfo> getNotExistColumnList() {
		return notExistColumnList;
	}

	public void setNotExistColumnList(List<ColumnInfo> notExistColumnList) {
		this.notExistColumnList = notExistColumnList;
	}
	
	public void addNotExistColumn(ColumnInfo columnInfo){
		Util.checkNull(columnInfo);
		this.notExistColumnList.add(columnInfo);
	}

	public List<String> getColumnList() {
		return columnList;
	}

	@Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
