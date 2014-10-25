package com.tracker.common.data.sortedObj;

public abstract class AdditionStored<K> extends Object{
	public abstract K getkey();
	public abstract void merge(AdditionStored value);
}
