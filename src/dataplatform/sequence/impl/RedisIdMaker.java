package dataplatform.sequence.impl;

import dataplatform.cache.ICache;
import dataplatform.sequence.IInstanceIdMaker;

/**
 * Redis-id生成器
 * @author	fuhuiyuan
 */
public class RedisIdMaker implements IInstanceIdMaker {
	
	private final ICache<String, String, Integer> cache;
	/**名称*/
	private final String name;
	
	public RedisIdMaker(String name, ICache<String, String, Integer> cache, int baseValue) {
		this.cache = cache;
		this.name = name;
		
		if (!cache.key().exists(name)) {
			for (int i = 0;i < baseValue;i++) {
				nextInstanceId();
			}
		}
	}

	@Override
	public int nextInstanceId() {
		return (int) cache.value().incr(name, 1);
	}

}
