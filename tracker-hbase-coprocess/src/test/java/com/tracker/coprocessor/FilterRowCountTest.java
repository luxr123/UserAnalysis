package com.tracker.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import com.tracker.coprocessor.generated.FilterRowCountProtos;
import com.tracker.coprocessor.generated.FilterRowCountProtos.CountRequest;
import com.tracker.coprocessor.generated.FilterRowCountProtos.CountResponse;
import com.tracker.coprocessor.generated.FilterRowCountProtos.RowCountService;

/**
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年9月12日 下午2:28:43]
 * @Version: [v1.0]
 * 
 */
public class FilterRowCountTest {

	public static void main(String[] args) throws Exception {
		System.out.println("begin.....");
		Configuration config = HBaseConfiguration.create();
		// String master_ip="192.168.150.128";
		String master_ip = "10.100.50.163";
		String zk_ip = "10.100.2.92,10.100.2.93,10.100.2.67";
		String table_name = "test_shell";
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", zk_ip);
		config.set("hbase.master", master_ip + ":600000");

		HTable table = new HTable(config, table_name);
		FilterRowCountProtos.CountRequest.Builder builder = CountRequest.newBuilder();
		org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("9223370636*"));
		scan.setFilter(filter);
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		int index = 0;
		builder.setScan(proto);
		builder.setFieldIndex(index);
		final CountRequest request = builder.build();
		try {
			Map<byte[], List<FilterRowCountProtos.KV>> results = table.coprocessorService(RowCountService.class, null, null,
					new Batch.Call<RowCountService, List<FilterRowCountProtos.KV>>() {

						@Override
						public List<FilterRowCountProtos.KV> call(RowCountService instance) throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<CountResponse> rpccall = new BlockingRpcCallback<CountResponse>();
							instance.getRowCount(controller, request, rpccall);
							CountResponse response = rpccall.get();
							return response.getKvList();
						}
					});

			for (Entry<byte[], List<FilterRowCountProtos.KV>> entry : results.entrySet()) {
				System.out.println("============================");
				System.out.println("key:" + Arrays.toString(entry.getKey()));
				List<FilterRowCountProtos.KV> list = entry.getValue();
				for(FilterRowCountProtos.KV k : list){
					System.out.println("kv-k:" + k.getKey());
					System.out.println("kv-v:" + k.getValue());
				}
				System.out.println("============================");
			}

		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
