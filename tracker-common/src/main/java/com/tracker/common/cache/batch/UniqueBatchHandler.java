package com.tracker.common.cache.batch;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
/**
 * 
 * 文件名：Batch
 * 创建人：jason.hua
 * 创建日期：2014-10-17 下午3:51:15
 * 功能描述：公共批处理抽象类， 用于定时、定量处理数据
 * 
 * example:
 * 	BatchHandler<String> b = new BatchHandler<String>(5, 1, TimeUnit.SECONDS) {
 *		public void flush(List<String> values) {
 *			System.out.println("batch: " + values);
 *		}
 *	};
 *	for (int i = 0; i < 99; i++) b.add(i + " ");
 */
public abstract class UniqueBatchHandler<T> {
	private int batchSize; //定义的每次批处理数量的界线
	private Set<T> cacheSet; //缓存待处理数据
    private ScheduledExecutorService executor; //定时线程服务
	private Lock lock = new ReentrantLock();// 锁 

	/**
	 * 
	 * 构造方法的描述.
	 * @param batchSize
	 * @param period time in milliseconds
	 */
	public UniqueBatchHandler(int batchSize, int period){
		this.batchSize = batchSize;
		cacheSet = new HashSet<T>(batchSize);
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
	 * 添加需要处理的数据
	 * @param obj
	 */
	public void add(T obj){
		lock.lock();
		if(!cacheSet.contains(obj)){
			cacheSet.add(obj);
		}
		if(cacheSet.size() >= batchSize){
			checkFlush();
		}
		lock.unlock();
	}
	
	/**
	 * check是否达到处理数据的要求，如果达到则进行处理
	 * @param checkSize
	 */
	private void checkFlush(){
		if(cacheSet.size() == 0)
			return;
		flush(cacheSet);
		cacheSet.clear();
	}
	
	public void close(){
		checkFlush();
		if(executor != null){
			executor.shutdownNow();
		}
	}
	
	
	/**
	 * 抽象的处理函数
	 * @param batch
	 */
	protected abstract void flush(Set<T> batch);
	
	
	public static void main(String[] args) throws InterruptedException {
		UniqueBatchHandler<String> b = new UniqueBatchHandler<String>(5, 1) {
			public void flush(Set<String> ints) {
				System.out.println("batch: " + ints);
			}
		};
		
		for (int i = 0; i < 99; i++) b.add(i + " ");
		Thread.sleep(3000);
		for (int i = 0; i < 99; i++) b.add(i + " ");
	}
}
