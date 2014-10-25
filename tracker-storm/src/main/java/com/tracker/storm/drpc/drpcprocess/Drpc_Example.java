package com.tracker.storm.drpc.drpcprocess;
//this is no longer used just for sampling
//Drpc request field value
//CLIENT-->DRPC SERVER-->TRANSPORT BOLT-->REAL TIME BOLT -->this-->Aggregate Bolt-->CLIENT
import java.io.Serializable;
import java.util.Calendar;
import java.util.Properties;
import java.util.Set;

import com.tracker.common.utils.ConfigExt;
import com.tracker.common.utils.StringUtil;
import com.tracker.storm.drpc.drpcresult.DrpcResult;

public class Drpc_Example extends DrpcProcess implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7654832864335215661L;
	private Properties m_properties;
	public static String ProcessFunc = "toprecord";
	
	public Drpc_Example(){
		// get cluster infomation
		String hdfsLocation = java.lang.System.getenv("HDFS_LOCATION");
		String configFile = java.lang.System.getenv("COMMON_CONFIG");
		m_properties = ConfigExt.getProperties(hdfsLocation,
				configFile);
	}
	
	
	@Override
	public boolean isProcessable(String input) {
		// TODO Auto-generated method stub
		boolean retVal = false;
		String splits[]  = input.split(StringUtil.ARUGEMENT_SPLIT);
		if(input.contains("toprecord") && splits.length == 6){ //toprecord:ip/cost/:Engine:SearchType:startIndex:endIndex
			retVal = true;
		}
		return retVal;
	}

	@Override
	public DrpcResult process(String input, Object localbuff) {
		// TODO Auto-generated method stub
//		String splits[]  = input.split(StringUtil.ARUGEMENT_SPLIT);
//		Calendar cal = Calendar.getInstance();
//		TopRecordResult  trr = new TopRecordResult();
//		String key = (cal.get(Calendar.MONTH)  + 1) + StringUtil.ARUGEMENT_SPLIT + cal.get(Calendar.DAY_OF_MONTH)
//				+ StringUtil.ARUGEMENT_SPLIT + splits[1] + StringUtil.ARUGEMENT_SPLIT+ splits[2];
//		if(splits[1].equals("ip") || splits[3].equals("")){
//			
//		}else{
//			key += StringUtil.ARUGEMENT_SPLIT + splits[3];
//		}
//		Set<String> retStr = JedisUtil.getInstance(m_properties).SORTSET.zrevrange(key, Integer.parseInt(splits[4]), Integer.parseInt(splits[5]));
//		trr.setString(retStr);
//		trr.setTotal(JedisUtil.getInstance(m_properties).SORTSET.zlength(key));
//		return trr;
		return null;
	}

}
