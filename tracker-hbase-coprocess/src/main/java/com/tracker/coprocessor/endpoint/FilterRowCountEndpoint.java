package com.tracker.coprocessor.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.tracker.coprocessor.generated.*;
import com.tracker.coprocessor.generated.FilterRowCountProtos.*;

/**
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年9月12日 上午11:22:53]
 * @Version: [v1.0]
 * 
 */
public class FilterRowCountEndpoint extends FilterRowCountProtos.RowCountService implements Coprocessor, CoprocessorService {
	public static final char ch = 16;
	public static final String ROW_SPLIT = String.valueOf(ch); // char(16）用于分隔符
	private RegionCoprocessorEnvironment env;

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void getRowCount(RpcController controller, CountRequest request, RpcCallback<CountResponse> done) {
		CountResponse response = null;
		InternalScanner scanner = null;
		Map<String, Long> map = null;
		try {
			map = new HashMap<String, Long>();
			int fieldIndex = request.getFieldIndex();
			Scan scan = ProtobufUtil.toScan(request.getScan());
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			byte[] lastRow = null;
			do {
				hasMore = scanner.next(results);
				for (Cell kv : results) {
					byte[] currentRow = CellUtil.cloneRow(kv);
					String field = new String(currentRow).split(ROW_SPLIT)[fieldIndex];
					if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
						lastRow = currentRow;
						Long count = map.get(field);
						if (count == null)
							map.put(field, 1L);
						else
							map.put(field, ++count);
					}
				}
				results.clear();
			} while (hasMore);
			List<FilterRowCountProtos.KV> kvs = new ArrayList<FilterRowCountProtos.KV>();
			for (Entry<String, Long> entry : map.entrySet()) {
				FilterRowCountProtos.KV.Builder builder = KV.newBuilder();
				builder.setKey(entry.getKey());
				builder.setValue(entry.getValue());
				FilterRowCountProtos.KV kv = builder.build();
				kvs.add(kv);
			}
			response = CountResponse.newBuilder().addAllKv(kvs).build();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		done.run(response);
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {

	}
}
