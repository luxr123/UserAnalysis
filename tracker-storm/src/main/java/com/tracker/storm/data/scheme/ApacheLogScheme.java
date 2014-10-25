package com.tracker.storm.data.scheme;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.tracker.common.log.ApacheLog;
import com.tracker.common.log.ApachePVLog;
import com.tracker.common.utils.JsonUtil;

public class ApacheLogScheme implements Scheme {
	private static final long serialVersionUID = -4810256685849079695L;

	@Override
	public List<Object> deserialize(byte[] ser) {
		try{
			String json = new String(ser, "UTF-8");
			Map<String, Object> fieldValueMap = JsonUtil.parseJSON2Map(json);
			Object cookieIdObj = fieldValueMap.get(ApachePVLog.FIELDS.cookieId.toString());
			Object logTypeObj = fieldValueMap.get(ApacheLog.logTypeField);
			if(cookieIdObj != null && logTypeObj != null){
				return new Values(cookieIdObj.toString(), logTypeObj.toString(), json);
			}
		 } catch (UnsupportedEncodingException e) {
	            throw new RuntimeException(e);
	     }
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(FIELDS.cookieId.toString(), FIELDS.logType.toString(), FIELDS.logJson.toString());
	}
	
	public static enum FIELDS{
		cookieId, logType, logJson
	}
}
