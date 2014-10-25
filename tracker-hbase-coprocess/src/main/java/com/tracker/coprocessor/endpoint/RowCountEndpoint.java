/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.tracker.coprocessor.generated.RowCountProtos;
import com.tracker.coprocessor.generated.RowCountProtos.CountRequest;
import com.tracker.coprocessor.generated.RowCountProtos.CountResponse;

/**
 * 
 * @author jason.hua
 *
 */
public class RowCountEndpoint extends RowCountProtos.RowCountService
    implements Coprocessor, CoprocessorService {
  private RegionCoprocessorEnvironment env;

  public RowCountEndpoint() {
  }

  /**
   * Just returns a reference to this object, which implements the RowCounterService interface.
   */
  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void getRowCount(RpcController controller, CountRequest request,
  		RpcCallback<CountResponse> done) {
    CountResponse response = null;
    InternalScanner scanner = null;
    try {
      Scan scan = ProtobufUtil.toScan(request.getScan());
//      scan.setFilter(new FirstKeyOnlyFilter());
      Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("0000001*"));
      scan.setFilter(filter);
      scan.setCaching(500);
      scan.setCacheBlocks(false);
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMore = false;
      byte[] lastRow = null;
      long count = 0;
      do {
        hasMore = scanner.next(results);
        for (Cell kv : results) {
          byte[] currentRow = CellUtil.cloneRow(kv);
          if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
            lastRow = currentRow;
            count++;
          }
        }
        results.clear();
      } while (hasMore);

      response = CountResponse.newBuilder().setCount(count).build();
    } catch (IOException ioe) {
      ResponseConverter.setControllerException(controller, ioe);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    done.run(response);
  }

  /**
   * Stores a reference to the coprocessor environment provided by the
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
   * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
   * on a table region, so always expects this to be an instance of
   * {@link RegionCoprocessorEnvironment}.
   * @param env the environment provided by the coprocessor host
   * @throws IOException if the provided environment is not an instance of
   * {@code RegionCoprocessorEnvironment}
   */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment)env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
	  
  }

}
