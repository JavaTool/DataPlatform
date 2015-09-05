package dataplatform.cache.sequence.impl;

import java.util.concurrent.atomic.AtomicInteger;

import dataplatform.cache.sequence.IInstanceIdMaker;

/**
 * Java自动id生成器
 * @author	fuhuiyuan
 */
public class JavaAutoIdMaker implements IInstanceIdMaker {
	
	protected AtomicInteger id_gen = new AtomicInteger();

	@Override
	public int nextInstanceId() {
		return id_gen.incrementAndGet();
	}

}
