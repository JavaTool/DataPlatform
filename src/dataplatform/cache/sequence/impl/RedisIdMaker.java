package dataplatform.cache.sequence.impl;

import redis.clients.jedis.Jedis;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.sequence.IInstanceIdMaker;

/**
 * Redis-id生成器
 * @author	fuhuiyuan
 */
public class RedisIdMaker implements IInstanceIdMaker {
	
	private final CacheOnJedis<Jedis, Jedis> cache;
	/**名称*/
	private final String name;
	
	public RedisIdMaker(String name, CacheOnJedis<Jedis, Jedis> cache, int baseValue) throws Exception {
		this.cache = cache;
		this.name = name;
		Jedis jedis = cache.getJedisCommands();
		try {
			if (!jedis.exists(name)) {
				jedis.set(name, baseValue + "");
			}
		} finally {
			cache.useFinishJ(jedis);
		}
	}

	@Override
	public int nextInstanceId() {
		Jedis jedis = cache.getJedisCommands();
		try {
			return jedis.incr(name).intValue();
		} finally {
			cache.useFinishJ(jedis);
		}
	}

}
