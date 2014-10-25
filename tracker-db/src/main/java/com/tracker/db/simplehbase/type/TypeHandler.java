package com.tracker.db.simplehbase.type;

import com.tracker.db.simplehbase.annotation.Nullable;

/**
 * Convert java type object to hbase's bytes back and forth.
 * 
 * <pre>
 * In general, one type handler handle one specified java type.
 * A java type and its boxer type would be handled in one type handler.
 * </pre>
 * 
 * */
public interface TypeHandler {

    /**
     * Convert java object to hbase's column bytes.
     * 
     * @param type java's type.
     * @param value java's object.
     * @return hbase's column bytes.
     * */
    @Nullable
    public byte[] toBytes(@Nullable Object value);

    /**
     * Convert hbase's column bytes to java object.
     * 
     * @param type java object's type.
     * @param bytes hbase's column bytes.
     * @return java object.
     * */
    @Nullable
    public Object toObject(@Nullable byte[] bytes);
    
    public Object stringToObject(@Nullable byte[] bytes);
    
    public byte[] stringToBytes(@Nullable Object value);
    
}
