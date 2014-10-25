package com.tracker.common.cache;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;

/**
 * 本地缓存
 * 
 * example:
 * 
 * LocalCache<String, String> localCache = new LocalCache<String, String>(2); // expireAfterAccess base on seconds
 * localCache.put("南京大学", "中国\t江苏省\t南京市");
 * System.out.println(localCache.get("南京大学"));
 * 
 * localCache.getOrElse("expire", new Function<String, String>(){
 *		@Override
 *		public String apply(String key) {
 *				return "test";
 *		}
 *			
 * });
 * @author jason.hua
 *
 * @param <K>
 * @param <V>
 */
public class LocalCache<K, V> {
	private static ConcurrentLinkedQueue<Cache<?, ?>> caches = new ConcurrentLinkedQueue<Cache<?, ?>>();
	private Cache<K, V> cache ;
	
	public LocalCache(long expireAfterAccess) {
		this(10000, expireAfterAccess);
	}
	
	public LocalCache(long maximumWeight, long expireAfterAccess) {
		cache = CacheBuilder.newBuilder()
				.maximumWeight(maximumWeight)
				.expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
				.weigher(new Weigher<K, V>() {
					@Override
					public int weigh(K key, V value) {
						 return 1;
					}
				})
				.concurrencyLevel(16)
//				.recordStats()
				.build();
		caches.add(cache);
	}
	
	public static CacheStats getStats() {
		CacheStats cs = new CacheStats(0, 0, 0, 0, 0, 0);
		for (Cache<?, ?> cache: caches) {
			cs = cs.plus(cache.stats());
		}
		return cs;
	}
	
	public void refresh(K key){  
		cache.invalidate(key);
	}
	
	public V get(K key){
		V value = cache.getIfPresent(key);
		return value;
	}
	
	public void put(K key, V value){
		cache.put(key, value);
	}
	
	public V getOrElse(K key, Function<K, V> load) {
		V value = cache.getIfPresent(key);
		if (value == null) {
			value = load.apply(key);
			if (value == null) return value;
			cache.put(key, value);
		}
		return value;
	}
	
	public static void main(String[] args) throws InterruptedException {
		LocalCache<String, String> localCache = new LocalCache<String, String>(2);
		localCache.put("南京大学", "中国\t江苏省\t南京市");
		
		localCache.getOrElse("expire", new Function<String, String>(){
			@Override
			public String apply(String key) {
				return "";
			}
			
		});
		
		System.out.println(localCache.get("南京大学"));
		Thread.sleep(1000 * 3);
		System.out.println(localCache.get("南京大学"));
		
	}
}