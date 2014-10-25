package com.tracker.db.hbaseCustomFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * 文件名：DateRowFilter
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午10:05:24
 * 功能描述：hbase的自定义过滤器,该过滤其已不在使用,可以作为以后的参考类
 *
 */
public class DateRowFilter extends FilterBase {
	public static enum WHEN {
		BEFOR, AFTER
	}
	public static enum TYPE {
		NEW,OLD
	}

	private final static DateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private String m_date;
	private Long m_dateToLong;
	private Long m_fromDate;
	private Long m_toDate;
	private TYPE m_type;
	private final Long oneDayMillis = (long) (60 * 60 * 24 * 1000);

	public static long parseTimeToLong(String original) {
		try {
			return dateFormat.parse(original).getTime();
		} catch (ParseException e) {
		}
		return System.currentTimeMillis();
	}

	public DateRowFilter() {
		super();
	}

	public DateRowFilter(String date,TYPE type){
		this(date, WHEN.AFTER, 1);
		m_type = type;
	}
	
	public DateRowFilter(String date) {
		this(date,TYPE.NEW);
	}

	public DateRowFilter(String date, WHEN when, int offset) {
		m_date = date;
		m_dateToLong = parseTimeToLong(m_date);
		if (offset <= 0) {
			offset = 1;
		}
		switch (when) {
		case BEFOR:
			m_fromDate = m_dateToLong - oneDayMillis * offset;
			m_toDate = m_dateToLong;
			break;
		case AFTER:
			m_fromDate = m_dateToLong;
			m_toDate = m_dateToLong + oneDayMillis * offset;
			break;
		default:
			break;
		}
		m_fromDate /= 1000;
		m_toDate /= 1000;
	}

	private boolean isInDate(byte[] rowKey) {
		boolean retVal = false;
		// parse rowkey to long type
		Long rowLong = new Long(Bytes.toString(rowKey));
//		System.out.println("compare value is : "  + rowLong);
		if (rowLong <= m_toDate && rowLong >= m_fromDate)
			retVal = true;
		return retVal;
	}
	
	@Override
	/**
	 * 过滤过程中每条记录最先被处理的函数
	 */
	public boolean filterRowKey(byte[] buffer, int offset, int length) {
		boolean retVal = true;
		try {
			int endpos = offset + length;
			for (int i = offset; i < endpos; i++) {
				if (buffer[i] == '-') {
					switch(m_type){
						//new custom
						case NEW:
							retVal = !(isInDate(Bytes.copy(buffer, offset, i - offset)));
							break;
						case OLD:
							retVal = isInDate(Bytes.copy(buffer, offset, i - offset));
							break;
					}
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}

	@Override
	public byte[] toByteArray() throws IOException {
		if(m_type == TYPE.NEW){
			m_date = "new-" + m_date;
		}
		else{
			m_date = "old-" + m_date;
		}
		return m_date.getBytes();
	}

	public static Filter parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		String str = new String(pbBytes);
		if(str.substring(0, 4).compareTo("new-") == 0)
			return new DateRowFilter(str.substring(4),TYPE.NEW);
		else 
			return new DateRowFilter(str.substring(4),TYPE.OLD);
	}

	boolean areSerializedFieldsEqual(Filter o) {
		if (o == this)
			return true;
		if (!(o instanceof DateRowFilter))
			return false;
		return true;
	}
	
//	public void write(DataOutput dataOutput) throws IOException {
//		Bytes.writeByteArray(dataOutput, this.m_date.getBytes());
//	}
//
//	public void readFields(DataInput dataInput) throws IOException {
//		this.m_date = new String(Bytes.readByteArray(dataInput));
//	}

	// for test
	public static void main(String[] args) {
		DateRowFilter dateFilter = new DateRowFilter("2014-05-27 00:00:00");
		String value = "1401175690-10.100.10.112-1401175690156827904";
		try {
			if (dateFilter.filterRowKey(value.getBytes(), 0, value.length())) {
				System.out.println(value + "not  in ");
			} else {
				System.out.println(value + " in ");
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
