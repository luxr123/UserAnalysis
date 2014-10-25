package com.tracker.db.simplehbase.request;

import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.util.Util;

/**
 * DeleteRequest.
 * 
 * */
public class DeleteRequest {

    private String rowKey;
    @Nullable
    private Long   timestamp;

    public DeleteRequest(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getRowKey() {
		return rowKey;
	}

	public DeleteRequest(String rowKey, long timestamp) {
        this.rowKey = rowKey;
        this.timestamp = timestamp;
    }

    public DeleteRequest(String rowKey, Date timestamp) {
        Util.checkNull(timestamp);
        this.rowKey = rowKey;
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

    public String getString() {
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
