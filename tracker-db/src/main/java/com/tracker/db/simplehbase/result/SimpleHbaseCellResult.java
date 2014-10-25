package com.tracker.db.simplehbase.result;

import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * SimpleHbaseCellResult.
 * 
 * <pre>
 * SimpleHbase mapping result on hbase cell.
 * </pre>
 * 
 * */
public class SimpleHbaseCellResult {

    private String rowKey;
    private String familyStr;
    private String qualifierStr;
    private Object valueObject;
    private Date   tsDate;

    public String getString() {
        return rowKey;
    }

    public void setString(String rowKey) {
        this.rowKey = rowKey;
    }

    public void setFamilyStr(String familyStr) {
        this.familyStr = familyStr;
    }

    public void setQualifierStr(String qualifierStr) {
        this.qualifierStr = qualifierStr;
    }

    public void setValueObject(Object valueObject) {
        this.valueObject = valueObject;
    }

    public void setTsDate(Date tsDate) {
        this.tsDate = tsDate;
    }

    public String getFamilyStr() {
        return familyStr;
    }

    public String getQualifierStr() {
        return qualifierStr;
    }

    public Object getValueObject() {
        return valueObject;
    }

    public Date getTsDate() {
        return tsDate;
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
