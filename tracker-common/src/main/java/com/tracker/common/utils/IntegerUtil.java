package com.tracker.common.utils;

public class IntegerUtil {
	  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

	/**
	   * Convert an int value to a byte array.  Big-endian.  Same as what DataOutputStream.writeInt
	   * does.
	   *
	   * @param val value
	   * @return the byte array
	   */
	  public static byte[] toBytes(int val) {
	    byte [] b = new byte[4];
	    for(int i = 3; i > 0; i--) {
	      b[i] = (byte) val;
	      val >>>= 8;
	    }
	    b[0] = (byte) val;
	    return b;
	  }

	  /**
	   * Converts a byte array to an int value
	   * @param bytes byte array
	   * @return the int value
	   */
	  public static int toInt(byte[] bytes) {
	    return toInt(bytes, 0, SIZEOF_INT);
	  }

	  /**
	   * Converts a byte array to an int value
	   * @param bytes byte array
	   * @param offset offset into array
	   * @return the int value
	   */
	  public static int toInt(byte[] bytes, int offset) {
	    return toInt(bytes, offset, SIZEOF_INT);
	  }

	  /**
	   * Converts a byte array to an int value
	   * @param bytes byte array
	   * @param offset offset into array
	   * @param length length of int (has to be {@link #SIZEOF_INT})
	   * @return the int value
	   * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
	   * if there's not enough room in the array at the offset indicated.
	   */
	  public static int toInt(byte[] bytes, int offset, final int length) {
	    if (length != SIZEOF_INT || offset + length > bytes.length) {
	      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
	    }
	    int n = 0;
	    for(int i = offset; i < (offset + length); i++) {
	      n <<= 8;
	      n ^= bytes[i] & 0xFF;
	    }
	    return n;
	  }
	  
	  private static IllegalArgumentException
	    explainWrongLengthOrOffset(final byte[] bytes,
	                               final int offset,
	                               final int length,
	                               final int expectedLength) {
	    String reason;
	    if (length != expectedLength) {
	      reason = "Wrong length: " + length + ", expected " + expectedLength;
	    } else {
	     reason = "offset (" + offset + ") + length (" + length + ") exceed the"
	        + " capacity of the array: " + bytes.length;
	    }
	    return new IllegalArgumentException(reason);
	  }
	  
	  public static void main(String[] args) {
		  int i = 4;
		  System.out.println(IntegerUtil.toInt(IntegerUtil.toBytes(i)));
	}
}
