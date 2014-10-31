package com.tracker.storm.drpc.drpcresult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.common.log.UserVisitLogFields;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.StringUtil;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem_hext;

public class RTRecordResult extends DrpcResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1305261002610535107L;
	public static final FIELDS fields[] = {FIELDS.serverLogTime};
	List<ValueItem_hext> m_result;
	
	public RTRecordResult(List<ValueItem_hext> results){
		m_result = results;
	}
	
	@Override
	public DrpcResult merge(DrpcResult part) {
		// TODO Auto-generated method stub
		if(part == null)
			return this;
		RTRecordResult tmpPart = (RTRecordResult)part;
		if(tmpPart.getResult().size() == 0)
			return this;
		m_result.addAll(tmpPart.getResult());
		Collections.sort(m_result, new Comparator<ValueItem_hext>(){
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
		return this;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String retVal = "";
		for(ValueItem_hext item : m_result){
			retVal += item.getName() + StringUtil.RETURN_ITEM_SPLIT;
		}
		return retVal;
	}

	@Override
	public Integer responseType() {
		// TODO Auto-generated method stub
		return 11;
	}
	
	public List<ValueItem_hext> getResult(){
		return m_result;
	}
	
	public static FIELDS[] getFields(){
		return fields;
	}

	public static class RecordItem extends UserVisitLogFields implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1810984452839570381L;
		
		
		public String toString() {
			// TODO Auto-generated method stub
			String retStr = "";
			for(FIELDS element: fields){
				retStr += element + StringUtil.KEY_VALUE_SPLIT +getString(element) + StringUtil.ARUGEMENT_SPLIT;
			}
			return retStr;
		}
		public Long getLong(FIELDS field){
			byte[] tmp = (byte[])getRaw(field);
			if(tmp != null){
				return Bytes.toLong(tmp);
			}else
				return -1L;
		}
		
		public String getString(FIELDS field){
			byte[] tmp = (byte[])getRaw(field);
			if(tmp != null){
				return Bytes.toString(tmp);
			}else
				return "";
		}
	}
}
