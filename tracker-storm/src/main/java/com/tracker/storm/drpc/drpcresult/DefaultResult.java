package com.tracker.storm.drpc.drpcresult;

import java.io.Serializable;

import com.tracker.common.utils.StringUtil;
/**
 * 
 * 文件名：DefaultResult
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:27:04
 * 功能描述：所有未知请求的结果类.
 *
 */
public class DefaultResult extends DrpcResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6055282062381369191L;
	private String message;
	
	public DefaultResult(String str){
		message = str;
	}
	
	@Override
	public DrpcResult merge(DrpcResult part) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return 0 + StringUtil.RETURN_ITEM_SPLIT + message;
	}

	@Override
	public Integer responseType() {
		// TODO Auto-generated method stub
		return 0;
	}

}
