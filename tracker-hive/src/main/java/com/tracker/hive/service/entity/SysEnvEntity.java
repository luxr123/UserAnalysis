package com.tracker.hive.service.entity;

import java.util.HashMap;
import java.util.Map;

import com.tracker.common.constant.website.SysEnvType;

/**
 * 文件名：SysEnvEntity
 * 创建人：zhengkang.gao
 * 创建日期：2014-10-21 下午4:15:32
 * 功能描述：系统环境
 *
 */
public class SysEnvEntity {
	private Map<SysEnvType, String> result = new HashMap<SysEnvType, String>();
	
	public SysEnvEntity(){
		
	}
	
	/**
	 * 函数名：addSysEnv
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 下午4:17:43
	 * 功能描述：添加系统环境
	 * @param sysEnvType
	 * @param name
	 */
	public void addSysEnv(SysEnvType sysEnvType, String name){
		result.put(sysEnvType, name);
	}

	public Map<SysEnvType, String> getResult() {
		return result;
	}

	/**
	 * 函数名：setResult
	 * 创建人：zhengkang.gao
	 * 创建日期：2014-10-21 下午4:21:12
	 * 功能描述：将参数result合并到本实例的result
	 * @param result
	 */
	public void setResult(Map<SysEnvType, String> result) {
		this.result.putAll(result);
	}
}
