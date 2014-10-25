package com.tracker.db.simplehbase.type.handler;

import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.db.simplehbase.type.TypeHandler;
import com.tracker.db.util.Util;
/**
 * @author jason.hua
 * */
public class ByteHandler implements TypeHandler {

	@Override
	public Object stringToObject(byte[] bytes) {
		return Byte.valueOf(Bytes.toString(bytes));
	}

	@Override
	public byte[] toBytes(Object value) {
		 return new byte[] { ((Byte) value).byteValue() };
	}

	@Override
	public Object toObject(byte[] bytes) {
		 Util.checkLength(bytes, 1);
	        return bytes[0];
	}
	
	@Override
	public byte[] stringToBytes(Object value) {
		return Bytes.toBytes(String.valueOf(value));
	}

}