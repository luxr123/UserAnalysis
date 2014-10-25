package com.tracker.storm.drpc.spottype;

import java.io.Serializable;

import com.tracker.storm.drpc.drpcprocess.DrpcProcess;
/**
 * 
 * 文件名：ClassItem
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月23日 下午3:42:52
 * 功能描述：用于实现DrpcProcess的动态构造,
 *
 */
public class ClassItem<T extends DrpcProcess> implements Serializable {


	private static final long serialVersionUID = -4993551482103032634L;
	private Object m_args[];
	private Class<T> m_class;
	public ClassItem(Object[] args,Class<T> item){
		m_args = args;
		m_class = item;
	}
	
	public Class<T> getObject(){
		return m_class;
	}
	
	public Object[] getArgs(){
		return m_args;
	}
	
}
