package com.tracker.storm.drpc.drpcprocess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.tracker.common.log.UserVisitLogFields.FIELDS;
import com.tracker.common.utils.RequestUtil;
import com.tracker.common.utils.StringUtil;
import com.tracker.storm.drpc.drpcresult.DrpcResult;
import com.tracker.storm.drpc.drpcresult.RTVisitorResult;
import com.tracker.storm.drpc.drpcresult.SearchValueResult.ValueItem;
/**
 * 
 * 文件名：RTUserProcess
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:15:44
 * 功能描述：功能与RTVisitorProcess一样.用于UserId的实时查询.
 * 返回的结果UserId唯一,这点区别于RTVisitorProcess
 *
 */
public class RTUserProcess extends RTVisitorProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6006805102420586508L;
	/**
	 * 返回userid,如果不存在着返回cookieId
	 */
	protected String getKeyWord(List<String> keys){
		String userId = keys.get(4);
		if(userId == null || userId.equals(""))
			return keys.get(3);
		else
			return   userId;
//		return keys.get(1);
	}

}
