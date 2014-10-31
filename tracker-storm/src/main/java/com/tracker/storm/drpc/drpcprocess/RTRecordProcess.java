package com.tracker.storm.drpc.drpcprocess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import com.tracker.storm.drpc.drpcresult.*;
import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.RequestUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.db.dao.webstats.model.WebSiteBaseTableAccess;
import com.tracker.db.dao.webstats.model.WebSiteFieldIndexAccess;
import com.tracker.db.hbase.HbaseCRUD.HbaseParam;
import com.tracker.db.hbase.HbaseCRUD.HbaseResult;
import com.tracker.storm.drpc.drpcresult.DrpcResult;
import com.tracker.storm.drpc.drpcresult.RTRecordResult;
import com.tracker.storm.drpc.drpcresult.RTRecordResult.RecordItem;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem_hext;

public class RTRecordProcess extends HBaseProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7963944432053206111L;
	public static final String ProcessFunc = RequestUtil.RTVisitorReq.RTRECORD_FUNC;
	@Override
	public boolean isProcessable(String input) {
		// TODO Auto-generated method stub
		boolean retVal = false;
		String splits[]  = input.split(StringUtil.ARUGEMENT_SPLIT);
		if(splits.length >= 7){
			retVal = true;
		}
		return retVal;
	}

	@Override
	public DrpcResult process(String input, Object localbuff) {
		// TODO Auto-generated method stub
		Map<String,Object> request = RequestUtil.RTVisitorReq.parseReq(input);
		Integer startIndex = (Integer) request.get(RequestUtil.STARTINDEX) - 1;
		Integer endIndex  = startIndex + (Integer) request.get(RequestUtil.COUNT) ;
		if(startIndex < 0 || endIndex < 0)
			return null;
		String userType = (String)request.get(FIELDS.userType.toString());
		String webId = (String)request.get(FIELDS.webId.toString());
		//get time range
		String date = (String) request.get(RequestUtil.DATETIME);
		Long startTime  = parseTimeToLong(date + " 00:00:00");
		Long endTime = parseTimeToLong(date + " 24:00:00");
		int pos = 0;
		List<ValueItem_hext> buf = new ArrayList<ValueItem_hext>();
		while(request.containsKey(RequestUtil.PARTITION + pos)){
			String partition = (String)request.get(RequestUtil.PARTITION + pos++);
			String startRowKey = webId + StringUtil.ARUGEMENT_SPLIT + partition;
			List<ValueItem_hext> tmp = getValueList(startTime, endTime, startRowKey, userType,endIndex);
			buf.addAll(tmp);
		}
		Collections.sort(buf, new Comparator<ValueItem_hext>(){
			@Override
			public int compare(ValueItem_hext o1, ValueItem_hext o2) {
				// TODO Auto-generated method stub
				long t1 = o1.getTimestamp();
				long t2 = o2.getTimestamp();
				if(t2 > t1)
					return 1;
				else if(t2 == t1)
					return 0;
				else 
					return -1;
			}
		});
		RTRecordResult retVal = new RTRecordResult(buf);
		return retVal;
	}
	
	protected List<ValueItem_hext> getValueList(Long startTime,Long endTime,String startRowKey
			,String userType,Integer endIndex){
		List<ValueItem_hext> retVal = new ArrayList<ValueItem_hext>();
		HbaseParam param = new HbaseParam();
		HbaseResult hresult = new HbaseResult();
		String nextKey = null;
		//filter user Type: add userType on rowkey
		if(userType != null && !userType.equals("") && !userType.equals("0")){
			SingleColumnValueFilter scvf = new SingleColumnValueFilter("infomation".getBytes(),
					FIELDS.userType.toString().getBytes(), CompareOp.EQUAL, Bytes.toBytes(userType));
			param.addFilter(scvf);
		}
		param.setColumns(UserVisitLogFields.castToList(RTRecordResult.fields));
		String endRowKey = startRowKey + ".";
		startRowKey += StringUtil.ARUGEMENT_SPLIT;
		if(startTime != null && endTime != null)
			param.setTimeRange(startTime, endTime);
		do{
			m_crud.setReturnSize(endIndex);
			nextKey = m_crud.readRange(param, hresult, startRowKey, endRowKey);
			do{
				String rowKey = WebSiteBaseTableAccess.getBaseTableRowKey(hresult);
				if(rowKey == null)
					continue;
				Long timeStamp = 0L;
				byte[] tmp = WebSiteBaseTableAccess.getBaseTableField(hresult, FIELDS.serverLogTime);
				if(tmp != null)
					timeStamp = Bytes.toLong(tmp);
				else
					continue;
				retVal.add(new ValueItem_hext(rowKey,timeStamp,0L));
				if(retVal.size() >= endIndex)
					return retVal;
			}while(WebSiteFieldIndexAccess.moveNext(hresult));
			startRowKey = nextKey;
		}while(nextKey != null);
		return retVal;
	}
}
