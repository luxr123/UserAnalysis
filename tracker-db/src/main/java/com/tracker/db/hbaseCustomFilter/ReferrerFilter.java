package com.tracker.db.hbaseCustomFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.common.utils.ConfigExt;
import com.tracker.db.hbase.HbaseUtils;
/**
 * 
 * 文件名：ReferrerFilter
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午10:03:42
 * 功能描述：hbase的自定义过滤器,该过滤器已不在使用,作为以后的参考类
 *
 */
public class ReferrerFilter extends FilterBase {
	protected byte[] columnFamily;
	protected byte[] columnQualifier;
	protected long refType;
	protected byte[] searchDomain;
	protected long timeStamp;
	protected boolean findSrc;
	protected boolean matchedColumn;
	protected boolean foundColumn;
	private final long twentyMinutes = 20 * 60 * 1000;
	private static byte[] refFamily = "extinfomation".getBytes();
	private static byte[] refQualifier = "refDomain".getBytes();

	public ReferrerFilter(final byte[] family, final byte[] qualifier,
			long refType, final byte[] searchDomain) {
		this.columnFamily = family;
		this.columnQualifier = qualifier;
		this.refType = refType;
		this.searchDomain = searchDomain;
		this.foundColumn = false;
		matchedColumn = false;
		timeStamp = -1;
	}

	public ReferrerFilter() {
		super();
	}

	@Override
	/**
	 * filterRowKey之后被调用的函数
	 */
	public ReturnCode filterKeyValue(Cell ignored) throws IOException {
		KeyValue keyValue = KeyValueUtil.ensureKeyValue(ignored);
		long kvTimeStamp = ignored.getTimestamp();
		if (this.matchedColumn) {
			if (kvTimeStamp >= timeStamp) {
				return ReturnCode.INCLUDE;
			} else {
				return ReturnCode.NEXT_COL;
			}
		}
		if (((ignored.getQualifierLength() != this.columnQualifier.length) ||
				ignored.getFamilyLength() != this.columnFamily.length) ||
				!(Bytes.equals(ignored.getQualifierArray(),ignored.getQualifierOffset(),ignored.getQualifierLength(),
						this.columnQualifier,0,this.columnQualifier.length)) ||
				!(Bytes.equals(ignored.getFamilyArray(),ignored.getFamilyOffset(),ignored.getFamilyLength(),
						this.columnFamily,0,this.columnFamily.length))) {
			if (foundColumn == true) {
				if (refType == 1)
					matchedColumn = true;
				else
					return ReturnCode.NEXT_ROW;
			}
			if (kvTimeStamp >= timeStamp) {
				return ReturnCode.INCLUDE;
			} else {
				return ReturnCode.NEXT_COL;
			}
		}
		// find the specified column,check the virsions
		foundColumn = true;
		String refVal = Bytes.toString(keyValue.getValueArray(),
				keyValue.getValueOffset(), keyValue.getValueLength());
		long type = Long.parseLong(refVal);
		if (type != 1) {
			// the session head
			if (timeStamp == -1 || timeStamp - kvTimeStamp < twentyMinutes) {
				// include current Cell
				if (refType == type) {
					matchedColumn = true;
					timeStamp = kvTimeStamp;
					return ReturnCode.INCLUDE_AND_NEXT_COL;
				} else {
					foundColumn = false;
					return ReturnCode.NEXT_ROW;
				}
			} else {
				// exclude current Cell
				if (refType == 1) {
					matchedColumn = true;
					return ReturnCode.NEXT_COL;
				} else {
					return ReturnCode.NEXT_ROW;
				}
			}
		} else {
			if (timeStamp == -1 || timeStamp - kvTimeStamp < twentyMinutes) {
				// include currentCell
				timeStamp = kvTimeStamp;
				return ReturnCode.INCLUDE;
			} else {
				// session
				// exclude current Cell
				if (refType == 1) {
					matchedColumn = true;
					return ReturnCode.NEXT_COL;
				} else {
					return ReturnCode.NEXT_ROW;
				}
			}
		}
	}

	@Override
	/**
	 * 提供修改行中单元的功能,只有在扫描完整行时才会被调用,可以通过设置hasFilterRow返回true来表示完整行
	 */
	public void filterRowCells(List<Cell> kvs) throws IOException {
		if (foundColumn == true && refType == 1) {
			matchedColumn = true;
		}

		if (matchedColumn == false)
			return;

		// filter the illegal cell
		List<Cell> list = new ArrayList<Cell>();
		for (Cell kv : kvs) {
			long kvtimStamp = kv.getTimestamp();
			if (timeStamp > kvtimStamp) {
				list.add(kv);
			}
		}
		// remove the illegal timestamp cell
		if(list.size() != 0){
			System.out.println("exclude cell");
			for (Cell i : list) {
				System.out.println("\t" + Bytes.toString(i.getRowArray(), i.getRowOffset(), i.getRowLength()) + 
						":" + Bytes.toString(i.getValueArray(), i.getValueOffset(), i.getValueLength()) );
				kvs.remove(i);
			}
			System.out.println();
		}

		if (matchedColumn && refType == 2) {
			// check the search engine
			for (Cell kv : kvs) {
				long kvtimStamp = kv.getTimestamp();
				if (timeStamp != kv.getTimestamp())
					continue;
				boolean bfamily = Bytes.equals(refFamily, 0, refFamily.length,
						kv.getFamilyArray(), kv.getFamilyOffset(),
						kv.getFamilyLength());
				boolean bquality = Bytes.equals(refQualifier, 0,
						refQualifier.length, kv.getQualifierArray(),
						kv.getQualifierOffset(), kv.getQualifierLength());
				if (bfamily && bquality) {
					try {
						matchedColumn = Bytes.equals(
								searchDomain,
								Bytes.copy(kv.getValueArray(),
										kv.getValueOffset(),
										kv.getValueLength()));
					} catch (Exception e) {
						System.out
								.println("error while parse the searchEngine Field of Cell");
					}
					break;
				}
			}
		}

	}

	@Override
	/**
	 * 在返回true的情况下filterRowCells才会被调用,filterRow
	 */
	public boolean hasFilterRow() {
		return true;
	}

	@Override
	public boolean filterRow() throws IOException {
		return !matchedColumn;
	}

	@Override
	public void reset() {
		matchedColumn = false;
		foundColumn = false;
		timeStamp = -1;
	}

	@Override
	/**
	 * 类对象的序列化
	 */
	public byte[] toByteArray() throws IOException {
		byte[] cfq = (new String(columnFamily) + ":" + new String(
				columnQualifier)).getBytes();
		return Bytes.add(
				Bytes.add(cfq, ":".getBytes(), Bytes.vintToBytes(refType)),
				":".getBytes(), searchDomain);
	}

	/**
	 * 类对象的反序列化
	 */
	public static Filter parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		byte[] family = Bytes.copy(pbBytes, 0,
				Bytes.indexOf(pbBytes, ":".getBytes()));
		byte[] tailBytes = Bytes.tail(pbBytes, pbBytes.length - family.length
				- 1);
		byte[] qualify = Bytes.copy(tailBytes, 0,
				Bytes.indexOf(tailBytes, ":".getBytes()));
		tailBytes = Bytes
				.tail(tailBytes, tailBytes.length - qualify.length - 1);
		byte[] refType = Bytes.copy(tailBytes, 0,
				Bytes.indexOf(tailBytes, ":".getBytes()));
		byte[] domain = Bytes.tail(tailBytes, tailBytes.length - refType.length
				- 1);
		return new ReferrerFilter(family, qualify, refType[0], domain);
	}

	boolean areSerializedFieldsEqual(Filter o) {
		if (o == this)
			return true;
		if (!(o instanceof ReferrerFilter))
			return false;
		return true;
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws DeserializationException
	 */
	public static void main(String[] args) throws IOException,
			DeserializationException {
		// TODO Auto-generated method stub
	        String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
	        String configFile = java.lang.System.getenv("COMMON_CONFIG");
	        Properties properties = ConfigExt.getProperties(hdfsLocation, configFile);
	        String zookeeper = properties.getProperty("hbase.zookeeper.quorum");
		HConnection conn = HbaseUtils.getHConnection(zookeeper);
		HTable table = new HTable(conn.getConfiguration(), "table".getBytes());
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.addColumn("extinfomation".getBytes(), "refType".getBytes());
		scan.addColumn("extinfomation".getBytes(), "refDomain".getBytes());
		ReferrerFilter rf = new ReferrerFilter("extinfomation".getBytes(),
				"refType".getBytes(), 2, "baidu.com".getBytes());
		scan.setFilter(rf);
		ResultScanner results = table.getScanner(scan);
		for (Result rs : results) {
			System.out.print(Bytes.toString(rs.getRow()) + "	");
			for (KeyValue kv : rs.list()) {
				System.out.print(Bytes.toString(kv.getValueArray(),
						kv.getValueOffset(), kv.getValueLength()));
				System.out.print("	");
			}
			System.out.println();
		}
	}
}
