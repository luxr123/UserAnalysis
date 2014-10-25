package com.tracker.db.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.simplehbase.request.DeleteRequest;
import com.tracker.db.simplehbase.request.PutRequest;

/**
 * Client Util.
 * 
 * @author xinzhi
 * */
public class Util {

    /**
     * Check boolean is NOT false.
     * */
    public static void check(boolean bool) {
        if (bool == false) {
            throw new SimpleHBaseException("bool is false.");
        }
    }

    /**
     * Check object is NOT null.
     * */
    public static void checkNull(Object obj) {
        if (obj == null) {
            throw new SimpleHBaseException("obj  is null.");
        }
    }

    /**
     * Check for string is NOT null or empty string.
     * */
    public static void checkEmptyString(String str) {
        if (StringUtil.isEmptyString(str)) {
            throw new SimpleHBaseException("str is null or empty.");
        }
    }

    /**
     * Check the value's length.
     * */
    public static void checkLength(byte[] values, int length) {
        Util.checkNull(values);

        if (values.length != length) {
            throw new SimpleHBaseException("checkLength error. values.length="
                    + values.length + " length=" + length);
        }
    }

    /**
     * Check string's length.
     * */
    public static void checkLength(String str, int length) {
        Util.checkNull(str);

        if (str.length() != length) {
            throw new SimpleHBaseException("checkLength error. str=" + str
                    + " length=" + length);
        }
    }

    /**
     * Check rowKey.
     * 
     * <pre>
     * rowKey is not null.
     * the result of rowKey's toBytes is not null.
     * </pre>
     * 
     * */
    public static void checkRowKey(String rowKey) {
        checkNull(rowKey);

        if (rowKey.length() == 0) {
            throw new SimpleHBaseException("rowkey bytes is null. rowKey = "
                    + rowKey);
        }
    }
    
    public static void checkRowKey(List<String> rowKeyList) {
        checkNull(rowKeyList);
        
        for(String rowKey: rowKeyList){
        	  checkNull(rowKey);
            if (rowKey.length() == 0) {
                throw new SimpleHBaseException("rowkey bytes is null. rowKey = "
                        + rowKey);
            }
        }
    }

    /**
     * Check put request.
     * */
    public static void checkPutRequest(PutRequest<?> putRequest) {
        checkNull(putRequest);
        checkRowKey(putRequest.getRowKey());
        checkNull(putRequest.getT());
    }

    /**
     * Check Delete request.
     * */
    public static void checkDeleteRequest(DeleteRequest deleteRequest) {
        checkNull(deleteRequest);
        checkRowKey(deleteRequest.getRowKey());
    }

    /**
     * Check for 2 objects have identity type.
     * */
    public static void checkIdentityType(Object one, Object other) {
        checkNull(one);
        checkNull(other);

        if (one.getClass() != other.getClass()) {
            throw new SimpleHBaseException("not same type. one = " + one
                    + " other = " + other);
        }
    }
    
    public static void checkZeroValue(Integer value){
    	checkNull(value);
    	 if (value == 0) {
             throw new SimpleHBaseException("value is zero");
         }
    }
    
    public static void checkPositiveValue(Integer value){
    	checkNull(value);
    	 if (value <= 0) {
             throw new SimpleHBaseException("value is <= 0");
         }
    }

    /**
     * Check for 2 objects are equal.
     * */
    public static void checkEquals(Object one, Object other) {
        if (one == other) {
            return;
        }

        if (one == null || other == null) {
            throw new SimpleHBaseException("null object. one = " + one
                    + " other = " + other);
        }
        if (!one.equals(other)) {
            throw new SimpleHBaseException("not equal object. one = " + one
                    + " other = " + other);
        }
    }

    /**
     * Close Closeable.
     * */
    public static void close(@Nullable Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            throw new SimpleHBaseException("close closeable exception.", e);
        }
    }

    private Util() {
    }
}
