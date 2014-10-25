package com.tracker.db.redis;

import java.util.LinkedList;
import java.util.List;
import java.util.ResourceBundle;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

public class JedisBase {
	public static JedisPool pool;  
	static {  
	    ResourceBundle bundle = ResourceBundle.getBundle("redis");  
	    if (bundle == null) {  
	        throw new IllegalArgumentException(  
	                "[redis.properties] is not found!");  
	    }  
	    JedisPoolConfig config = new JedisPoolConfig();  
	    config.setMaxTotal(Integer.valueOf(bundle  
	            .getString("redis.pool.maxActive")));  
	    config.setMaxIdle(Integer.valueOf(bundle  
	            .getString("redis.pool.maxIdle")));  
	    config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));  
	    config.setTestOnBorrow(Boolean.valueOf(bundle  
	            .getString("redis.pool.testOnBorrow")));  
	    config.setTestOnReturn(Boolean.valueOf(bundle  
	            .getString("redis.pool.testOnReturn")));  
	    pool = new JedisPool(config, bundle.getString("redis.master.ip"),  
	            Integer.valueOf(bundle.getString("redis.master.port")),10000,bundle.getString("redis.master.password"));  
	}  
	
	private static ShardedJedisPool shardedPool;  
	static {  
	    ResourceBundle bundle = ResourceBundle.getBundle("redis");  
	    if (bundle == null) {  
	        throw new IllegalArgumentException(  
	                "[redis.properties] is not found!");  
	    }  
	    JedisPoolConfig config = new JedisPoolConfig();  
	    config.setMaxTotal(Integer.valueOf(bundle  
	            .getString("redis.pool.maxActive")));  
	    config.setMaxIdle(Integer.valueOf(bundle  
	            .getString("redis.pool.maxIdle")));  
	    config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));  
	    config.setTestOnBorrow(Boolean.valueOf(bundle  
	            .getString("redis.pool.testOnBorrow")));  
	    config.setTestOnReturn(Boolean.valueOf(bundle  
	            .getString("redis.pool.testOnReturn")));  
	    JedisShardInfo jedisShardInfo1 = new JedisShardInfo(  
                bundle.getString("redis.master.ip"), Integer.valueOf(bundle.getString("redis.master.port")));  
	    jedisShardInfo1.setPassword(bundle.getString("redis.master.password"));
	    JedisShardInfo jedisShardInfo2 = new JedisShardInfo(  
                bundle.getString("redis.slave.ip"), Integer.valueOf(bundle.getString("redis.slave.port"))); 
	    jedisShardInfo2.setPassword(bundle.getString("redis.slave.password"));
	    List<JedisShardInfo> list = new LinkedList<JedisShardInfo>();  
	    list.add(jedisShardInfo1);  
	    list.add(jedisShardInfo2);  
	    shardedPool = new ShardedJedisPool(config, list);   
	}
	
	public static void main(String[] args) {
		//ShardedJedis jedis=shardedPool.getResource();
		Jedis jedis=pool.getResource();
		String keys = "name";
		String value="kris";
		jedis.del(keys);
		jedis.set(keys, value);
		
		String v=jedis.get(keys);
		System.out.println(v);
		//shardedPool.returnResource(jedis);
		pool.returnResource(jedis);
	}
}