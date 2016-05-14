package dataplatform.sequence.impl;

import dataplatform.cache.ICache;
import dataplatform.sequence.IInstanceIdMaker;

/**
 * 序列号生成器
 * @author	fuhuiyuan
 */
public class SequenceMaker implements IInstanceIdMaker {
	
	/**缓存器*/
	protected ICache<String, String, Integer> cache;
	/**名称*/
	protected String name;
	
	public SequenceMaker(ICache<String, String, Integer> cache, String name) {
		this.cache = cache;
		this.name = name;
	}

	@Override
	public synchronized int nextInstanceId() {
		Object value = cache.hash().get(name);
		return value == null ? 0 : Integer.parseInt(value.toString()) + 1;
	}

}
