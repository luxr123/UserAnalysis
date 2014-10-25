package com.tracker.storm.drpc.spottype;
/**
 * 
 * 文件名：DynamicConstruct
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午4:11:03
 * 功能描述：处理类动态创建后,需要额外初始化的话,实现该接口
 *
 */
public interface DynamicConstruct {
	public void init(Object args[]);
	
}
