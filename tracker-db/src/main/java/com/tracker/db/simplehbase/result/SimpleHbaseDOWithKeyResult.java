package com.tracker.db.simplehbase.result;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * DOWithKeyResult.
 * 
 * <pre>
 * All the cell on latest timestamp are mapped to one DO.
 * </pre>
 * 
 * */
public class SimpleHbaseDOWithKeyResult<T> {
    /** rowkey. */
    private String rowKey;
    /** The mapping result of DO. */
    private T      t;

    public void setT(T t) {
        this.t = t;
    }

    public T getT() {
        return t;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setString(String rowKey) {
        this.rowKey = rowKey;
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
