package dataplatform.sequence.impl;

import java.util.concurrent.TimeUnit;

import dataplatform.cache.ICache;
import dataplatform.sequence.ICounter;

public class RedisCounter implements ICounter {
	
	private final ICache<String, String, Integer> cache;
	
	public RedisCounter(ICache<String, String, Integer> cache) {
		this.cache = cache;
	}

	@Override
	public long getCount(String key) {
		return cache.value().get(key);
	}

	@Override
	public long incr(String key, long value, long time) {
		long ret = cache.value().incr(key, value);
		setTime(key, time);
		return ret;
	}
	
	protected void setTime(String key, long time) {
		if (time != NO_TIME) {
			cache.key().expire(key, time, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public long decr(String key, long value, long time) {
		long ret = cache.value().decr(key, value);
		setTime(key, time);
		return ret;
	}

	@Override
	public void deleteCount(String key) {
		cache.key().delete(key);
	}

	@Override
	public long getCount(String key, String name) {
		return cache.hash().get(key, name);
	}

	@Override
	public long incr(String key, String name, long value, long time) {
//		Jedis jedis = cache.getJedis();
//		try {
//			setTime(jedis, key, time);
//			return jedis.hincrBy(key, name, value);
//		} catch (Exception e) {
//			log.error("", e);
//			return 0L;
//		} finally {
//			cache.useFinish(jedis);
//		}
		return 0;
	}

	@Override
	public long decr(String key, String name, long value, long time) {
		return incr(key, name, -value, time);
	}

	@Override
	public void deleteCount(String key, String name) {
		cache.hash().remove(key, name);
	}

}
