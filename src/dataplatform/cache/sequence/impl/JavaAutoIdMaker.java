package dataplatform.cache.sequence.impl;

import java.util.concurrent.atomic.AtomicInteger;

import dataplatform.cache.sequence.IInstanceIdMaker;

/**
 * Java自动id生成器
 * @author	fuhuiyuan
 */
public class JavaAutoIdMaker implements IInstanceIdMaker {
	
	protected final AtomicInteger id_gen;
	
	public JavaAutoIdMaker() {
		this(0);
	}
	
	public JavaAutoIdMaker(int defaultValue) {
		id_gen = new AtomicInteger(defaultValue);
	}

	@Override
	public int nextInstanceId() {
		return id_gen.incrementAndGet();
	}

}
