package com.tracker.common.data.sortedObj;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AdditionHash<T extends AdditionStored<String>> extends
		AbstractSet<T> implements Set<T>, Cloneable, java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2026302909218584896L;
	private transient HashMap<String, T> map;

	public AdditionHash() {
		map = new HashMap<String, T>();
	}

	public AdditionHash(int cap) {
		map = new HashMap<String, T>(cap);
	}
	
	public List<T> getList(){
		if(map == null)
			return null;
		return new ArrayList<T>(map.values());
	}

	@Override
	public boolean add(T t) {
		if(t == null)
			return false;
		if(map == null)
			map =  new HashMap<String, T>();
		if(map.containsKey(t.getkey())){
			map.get(t.getkey()).merge(t);
			return false;
		}else{
			map.put(t.getkey(),t);
			return true;
		}
	}
	
	public T get(String item){
		if(map == null)
			return null;
		T ret = null;
		ret = map.get(item);
		return ret;
	}
	
	@Override
	public void clear() {
		if(map != null)
			map.clear();
	}

	@Override
	public int size() {
		if(map != null)
			return map.size();
		else
			return -1;
	}

	@Override
	public Iterator<T> iterator() {
		// TODO Auto-generated method stub
		if(map != null && map.values() != null)
			return map.values().iterator();
		else
			return   new HashMap<String, T>().values().iterator();
	}

}
