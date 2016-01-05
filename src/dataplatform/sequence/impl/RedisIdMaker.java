package dataplatform.sequence.impl;

import redis.clients.jedis.Jedis;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.sequence.IInstanceIdMaker;

/**
 * Redis-id生成器
 * @author	fuhuiyuan
 */
public class RedisIdMaker implements IInstanceIdMaker {
	
	private final CacheOnJedis cache;
	/**名称*/
	private final String name;
	
	public RedisIdMaker(String name, CacheOnJedis cache, int baseValue) throws Exception {
		this.cache = cache;
		this.name = name;
		Jedis jedis = cache.getJedis();
		try {
			if (!jedis.exists(name)) {
				jedis.set(name, baseValue + "");
			}
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public int nextInstanceId() {
		Jedis jedis = cache.getJedis();
		try {
			return jedis.incr(name).intValue();
		} finally {
			cache.useFinish(jedis);
		}
	}

}
