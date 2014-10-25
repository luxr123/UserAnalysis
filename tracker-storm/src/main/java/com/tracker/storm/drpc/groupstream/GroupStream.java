package com.tracker.storm.drpc.groupstream;

import java.util.List;
import com.tracker.storm.drpc.TransportBolt;
/**
 * 
 * 文件名：GroupStream
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月22日 下午5:28:00
 * 功能描述：所有Dprc请求转发类的父类
 *
 */

public interface GroupStream {
	public List<Object> group(TransportBolt transport,String request);
}
