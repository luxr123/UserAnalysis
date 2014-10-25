package com.tracker.db.simplehbase.request;

import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.util.Util;

/**
 * PutRequest.
 * 
 * */
public class PutRequest<T> {

    private String rowKey;
    private T      t;
    @Nullable
    private Long   timestamp;

    public PutRequest(String rowKey, T t) {
        this.rowKey = rowKey;
        this.t = t;
    }

    public PutRequest(String rowKey, T t, long timestamp) {
        this.rowKey = rowKey;
        this.t = t;
        this.timestamp = timestamp;
    }

    public PutRequest(String rowKey, T t, Date timestamp) {
        Util.checkNull(timestamp);

        this.rowKey = rowKey;
        this.t = t;
        this.timestamp = timestamp.getTime();
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTimestamp(Date timestamp) {
        Util.checkNull(timestamp);
        this.timestamp = timestamp.getTime();
    }

    public void cleanTimestamp() {
        this.timestamp = null;
    }

    public T getT() {
        return t;
    }

    public String getRowKey() {
		return rowKey;
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
