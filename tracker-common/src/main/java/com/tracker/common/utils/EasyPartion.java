package com.tracker.common.utils;
/**
 * 
 * 文件名：EasyPartion
 * 创建人：zhifeng.zheng
 * 创建日期：2014年10月24日 上午11:05:53
 * 功能描述：对索引表的数据进行分区,进行分布式计算
 *
 */
public class EasyPartion {
	public static Integer partitions = 15;
	public static Integer getPartition(String input){
		Integer retVal = 0;
		for(char c:input.toCharArray()){
			retVal +=c;
		}
		return retVal % partitions;
	}
}
