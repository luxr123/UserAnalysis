package com.tracker.coprocessor.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.tracker.coprocessor.generated.TopProtos;
import com.tracker.coprocessor.generated.TopProtos.KV;
import com.tracker.coprocessor.generated.TopProtos.TopRequest;
import com.tracker.coprocessor.generated.TopProtos.TopResponse;
import com.tracker.coprocessor.utils.DoublePriorityQueue;
/**
 * 
 * 文件名：TopEndpoint
 * 创建人：kris.chen
 * 创建日期：2014-10-14 下午4:58:11
 * 功能描述：取Top协处理器服务端处理代码
 *
 */
public class TopEndpoint extends TopProtos.TopService implements Coprocessor,CoprocessorService{
	private static Logger logger = LoggerFactory.getLogger(TopEndpoint.class);
	private RegionCoprocessorEnvironment env;
	public TopEndpoint(){
		
	}
	
	@Override
	public Service getService() {
		return this;
	}

	@Override
	/**
	 * 解析request，对分区表做查询取数据，对数据按从大到小排序后封装成response返回
	 */
	public void getTop(RpcController controller, TopRequest request,
			RpcCallback<TopResponse> done) {
		TopResponse response=null;
		InternalScanner scanner=null;
		try{
			Scan scan=ProtobufUtil.toScan(request.getScan());	//解析Scan
			scanner=env.getRegion().getScanner(scan);	//获取数据
			List<Cell> results=new ArrayList<Cell>();
			int topN=request.getTopN();
			DoublePriorityQueue<KV> dp=new DoublePriorityQueue<KV>(topN,true);	//用堆排序类对数据进行降序排序
			boolean hasMore=false;
			byte[] lastRow=null;
			int nRowCount=0;
			do{
				hasMore=scanner.next(results);
				for(Cell cell:results){
					byte[] currentRow=CellUtil.cloneRow(cell);
					if(lastRow == null ||!Bytes.equals(lastRow,currentRow)){
						KV kv= KV.newBuilder().setKey(new String(CellUtil.cloneRow(cell))).setValue(Bytes.toLong(CellUtil.cloneValue(cell))).build();
						dp.add(kv.getValue(),kv);
						//统计行数
						lastRow=currentRow;
						nRowCount++;
					}
				}
				results.clear();
			}while(hasMore);
			response=TopResponse.newBuilder().addAllKv(dp.values()).setDataSize(nRowCount).build();
		}catch(IOException ioe){
			ResponseConverter.setControllerException(controller, ioe);
		}finally{
			if (scanner != null) {
		        try {
		          scanner.close();
		        } catch (IOException ignored) {}
		    }
		}
		done.run(response);
	}
	
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
	      this.env = (RegionCoprocessorEnvironment)env;
	    } else {
	      throw new CoprocessorException("Must be loaded on a table region!");
	    }
	}

	@Override
	public void stop(CoprocessorEnvironment arg0) throws IOException {

	}

}
