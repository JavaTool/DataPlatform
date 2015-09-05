package dataplatform.cache.sequence.impl;

import dataplatform.cache.ICache;
import dataplatform.cache.sequence.IInstanceIdMaker;

/**
 * 序列号生成器
 * @author	fuhuiyuan
 */
public class SequenceMaker implements IInstanceIdMaker {
	
	/**缓存器*/
	protected ICache cache;
	/**名称*/
	protected String name;
	
	public SequenceMaker(ICache cache, String name) {
		this.cache = cache;
		this.name = name;
	}

	@Override
	public synchronized int nextInstanceId() {
		return ((Integer) cache.get(name)) + 1;
	}

}
