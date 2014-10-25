package com.tracker.common.cache.batch;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * 文件名：KVBatchHandler
 * 创建人：jason.hua
 * 创建日期：2014-10-17 下午3:51:15
 * 功能描述：公共批处理抽象类<K,V>， 用于定时、定量处理数据
 * 
 * example:
 * 		KVBatchHandler<Integer, Long> b = new KVBatchHandler<Integer, Long>(1500, 2) {
 * 			@Override
 * 			protected Long updateValue(Long firstVal, Long secondVal) {
 * 				return firstVal + secondVal;
 * 			}
 * 
 * 			@Override
 * 			protected void flush(ConcurrentHashMap<Integer, Long> cacheMap) {
 * 				System.out.println("-==============");
 * 				for(Integer key: cacheMap.keySet()){
 * 					System.out.println(key + " => " + cacheMap.get(key));
 * 				}
 * 			}
 * 		};
 * 		for (int i = 0; i < 99; i++) b.update(i, 1L);
 */
public abstract class KVBatchHandler<K, V>{
	private int batchSize; //定义的每次批处理数量的界线
	private ConcurrentHashMap<K, V> cacheMap; //缓存待处理数据
    private ScheduledExecutorService executor; //定时线程服务
	private Lock lock = new ReentrantLock();// 锁 
	/**
	 * 
	 * 构造方法的描述.
	 * @param batchSize
	 * @param period time in MILLISECONDS
	 */
	public KVBatchHandler(int batchSize, int period){
		this.batchSize = batchSize;
		cacheMap = new ConcurrentHashMap<K, V>();
		if(period > 0 && batchSize > 1){
			executor = Executors.newSingleThreadScheduledExecutor();
			executor.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					lock.lock();
					checkFlush();
					lock.unlock();
				}
			}, period, period, TimeUnit.MILLISECONDS);
		}
	}
	
	/**
	 * 
	 * 函数名：update
	 * 功能描述：根据key更新value值
	 * @param key
	 * @param value
	 */
	public void update(K key, V value){
		lock.lock();
		
		//更新value值
		if(cacheMap.containsKey(key)){
			V resultVal = updateValue(cacheMap.get(key), value);
			if(resultVal != null)
				cacheMap.put(key, resultVal);
		} else {
			cacheMap.put(key, value);
		}
		
		//检查并处理
		if(cacheMap.size() >= batchSize) {
			checkFlush();
		}
		
		lock.unlock();
	}
	
	/**
	 * check是否达到处理数据的要求，如果达到则进行处理
	 * @param checkSize
	 */
	private void checkFlush(){
		if(cacheMap.size() > 0) {
			flush(cacheMap);
			cacheMap.clear();
		}
	}
	
	public void close(){
		checkFlush();
		if(executor != null){
			executor.shutdownNow();
		}
	}
	
	/**
	 * 
	 * 函数名：updateValue
	 * 功能描述：合并firstVal、secondVal
	 * @param firstVal
	 * @param secondVal
	 * @return
	 */
	protected abstract V updateValue(V firstVal, V secondVal);
	
	/**
	 * 
	 * 函数名：flush
	 * 功能描述：处理缓存中的数据
	 * @param cacheMap
	 */
	protected abstract void flush(ConcurrentHashMap<K, V> cacheMap);
	
	
	
	public static void main(String[] args) throws InterruptedException {
		KVBatchHandler<Integer, Long> b = new KVBatchHandler<Integer, Long>(1500, 2) {
			@Override
			protected Long updateValue(Long firstVal, Long secondVal) {
				return firstVal + secondVal;
			}

			@Override
			protected void flush(ConcurrentHashMap<Integer, Long> cacheMap) {
				System.out.println("-==============");
				for(Integer key: cacheMap.keySet()){
					System.out.println(key + " => " + cacheMap.get(key));
				}
			}
		};
		for (int i = 0; i < 99; i++) b.update(i, 1L);
		for (int i = 40; i < 99; i++) b.update(i, 1L);
		for (int i = 50; i < 99; i++) b.update(i, 1L);
		for (int i = 60; i < 99; i++) b.update(i, 1L);
		
	}
}
