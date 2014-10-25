package com.tracker.coprocessor;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import com.tracker.coprocessor.generated.RowCountProtos.CountRequest;
import com.tracker.coprocessor.generated.RowCountProtos.CountRequest.Builder;
import com.tracker.coprocessor.generated.RowCountProtos.CountResponse;
import com.tracker.coprocessor.generated.RowCountProtos.RowCountService;

public class RowCountTest {
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.2.92,10.100.2.93,10.100.2.67");
		HTable table = new HTable(conf, "ip");
	
		Map<byte[], Long> results = table.coprocessorService(RowCountService.class, null, null,
				new Batch.Call<RowCountService, Long>() {
					public Long call(RowCountService counter)throws IOException {
						Builder builder = CountRequest.newBuilder();
						Scan scan = new Scan();
						builder.setScan(ProtobufUtil.toScan(scan));
						
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<CountResponse> rpcCallback = new BlockingRpcCallback<CountResponse>();
						counter.getRowCount(controller, builder.build(), rpcCallback);
						
						CountResponse response = rpcCallback.get();
						if (controller.failedOnException()) {
							throw controller.getFailedOn();
						}
						return (response != null && response.hasCount()) ? response
								.getCount() : 0;
					}
				});
		Long count = 0L;
		for(long val : results.values()){
			count += val;
		}
		System.out.println(count);
	}
}
