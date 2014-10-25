package com.tracker.common.utils;

import java.util.zip.CRC32;

/**
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年8月8日 下午2:05:26]
 * @Version: [v1.0]
 * 
 */
public class CRC20 {

	public static int getId(String s) {
		CRC32 crc32 = new CRC32();
		crc32.update(s.getBytes());
		return (int) (crc32.getValue() & 0xfffff);
	}
}
