package com.tracker.storm.drpc.drpcresult;
/**
 * 
 * 文件名：DrpcResult
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:26:15
 * 功能描述：所有Drpc请求结果类的父类.Aggregate类根据m_responseType值对返回结果
 * 进行合并,立即回复,废弃等操作.
 *
 */
public abstract class DrpcResult {
	protected Integer m_responseType;
	public DrpcResult(){
		m_responseType = 0;
	}
	public abstract DrpcResult merge (DrpcResult part);
	public abstract String toString();
	public abstract Integer responseType();
}
