package dataplatform.cache.sequence.impl;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.sequence.ICounter;

public class RedisCounter<B extends BinaryJedisCommands, J extends JedisCommands> implements ICounter {
	
	private final CacheOnJedis<B, J> cache;
	
	public RedisCounter(CacheOnJedis<B, J> cache) {
		this.cache = cache;
	}

	@Override
	public long get(String key) {
		J jedis = cache.getJedisCommands();
		try {
			return Long.parseLong(jedis.get(key));
		} finally {
			cache.useFinishJ(jedis);
		}
	}

	@Override
	public long incr(String key, long value) {
		J jedis = cache.getJedisCommands();
		try {
			return jedis.incrBy(key, value);
		} finally {
			cache.useFinishJ(jedis);
		}
	}

	@Override
	public long decr(String key, long value) {
		J jedis = cache.getJedisCommands();
		try {
			return jedis.decrBy(key, value);
		} finally {
			cache.useFinishJ(jedis);
		}
	}

	@Override
	public void delete(String key) {
		J jedis = cache.getJedisCommands();
		try {
			jedis.del(key);
		} finally {
			cache.useFinishJ(jedis);
		}
	}

}
