package com.tracker.common.utils;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * 一致性hash
 * @author kris.chen
 */
public class ConsistentHash<T> {
	private TreeMap<Long,T> circleNodes;//虚拟节点-hash环
	private List<T> realNodes;//真实节点
	private final int NODE_NUM=100;//每个机器节点关联的虚拟节点个数
	
	/**
	 * 构造函数
	 * @param List<T> realNodes
	 */
	public ConsistentHash(List<T> realNodes){
		super();
		this.realNodes=realNodes;
		init();
	}

	/**
	 * 初始化一致性hash环
	 */
	private void init(){
		circleNodes=new TreeMap<Long,T>();
		for(int i=0;i!=realNodes.size();i++){
			final T node=realNodes.get(i);
			for(int n=0;n<NODE_NUM;n++){
				//一个真实机器节点关联NODE_NUM个虚拟节点
				circleNodes.put(hash(node.toString()+"-NODE-"+n), node);
			}
		}
	}

	/**
	 * 获取服务节点
	 * @param String key
	 * @return T
	 */
	public T get(String key){
		SortedMap<Long,T> tail=circleNodes.tailMap(hash(key));//沿环顺时针找到一个虚拟节点
		if(tail.size()==0){
			return circleNodes.get(circleNodes.firstKey());
		}
		return tail.get(tail.firstKey());//返回该虚拟节点对应的真实机器的节点
	}
	
	/**
	 * 
	 * 删除服务节点
	 * @param T node
	 */
	public void remove(T node){
		//删除真实节点关联的所有虚拟节点
		for(int n=0;n<NODE_NUM;n++){
			circleNodes.remove(hash(node.toString()+"-NODE-"+n));
		}
	}
	
	
	/** 
	 *  MurMurHash算法，是非加密HASH算法，性能很高， 
	 *  比传统的CRC32,MD5，SHA-1（这两个算法都是加密HASH算法，复杂度本身就很高，带来的性能上的损害也不可避免） 
	 *  等HASH算法要快很多，而且据说这个算法的碰撞率很低. 
	 *  http://murmurhash.googlepages.com/ 
	 */  
	private Long hash(String key){
		ByteBuffer buf=ByteBuffer.wrap(key.getBytes());
		int seed=0x1234ABCD;
		
		ByteOrder byteOrder=buf.order();
		buf.order(byteOrder.LITTLE_ENDIAN);
		
		long m = 0xc6a4a7935bd1e995L; 
		int r=47;
		
		long h = seed ^ (buf.remaining() * m);
		long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();  
            k *= m;  
            k ^= k >>> r;  
            k *= m;  
            h ^= k;
            h *= m;
        }  
        if (buf.remaining() > 0) {  
            ByteBuffer finish = ByteBuffer.allocate(8).order(  
                    ByteOrder.LITTLE_ENDIAN);  
            // for big-endian version, do this first:   
            // finish.position(8-buf.remaining());   
            finish.put(buf).rewind();  
            h ^= finish.getLong();  
            h *= m;  
        }  
        h ^= h >>> r;  
        h *= m;  
        h ^= h >>> r;  
        
        buf.order(byteOrder);  
        return h;  
	}
}
