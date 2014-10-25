package com.tracker.storm.drpc.drpcprocess;

import com.tracker.storm.drpc.drpcresult.DrpcResult;
/**
 * 
 * 文件名：DrpcProcess
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:24:27
 * 功能描述：基础的处理类.
 *
 */
public abstract class DrpcProcess {
	public DrpcProcess(){};
	//用于检查请求的合法性
	public abstract boolean isProcessable(String input); //for test input argument is compati suittable to continue pass into process()
	//处理请求
	public abstract DrpcResult process(String input,Object localbuff);//return the request
	
}
