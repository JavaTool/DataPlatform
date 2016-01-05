package dataplatform.cache.sequence.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.sequence.ICounter;
import redis.clients.jedis.Jedis;

public class RedisCounter implements ICounter {
	
	protected static final Logger log = LoggerFactory.getLogger(RedisCounter.class);
	
	private final CacheOnJedis cache;
	
	public RedisCounter(CacheOnJedis cache) {
		this.cache = cache;
	}

	@Override
	public long getCount(String key) {
		Jedis jedis = cache.getJedis();
		try {
			String vaule = (String) jedis.get(key);
			return Long.parseLong(vaule == null ? "0" : vaule);
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public long incr(String key, long value, long time) {
		Jedis jedis = cache.getJedis();
		try {
			setTime(jedis, key, time);
			return jedis.incrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			cache.useFinish(jedis);
		}
	}
	
	protected void setTime(Jedis jedis, String key, long time) {
		if (time != NO_TIME) {
			jedis.pexpire(key, time);
		}
	}

	@Override
	public long decr(String key, long value, long time) {
		Jedis jedis = cache.getJedis();
		try {
			setTime(jedis, key, time);
			return jedis.decrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public void deleteCount(String key) {
		cache.del(key);
	}

	@Override
	public long getCount(String key, String name) {
		Jedis jedis = cache.getJedis();
		try {
			String vaule = (String) jedis.hget(key, name);
			return Long.parseLong(vaule == null ? "0" : vaule);
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public long incr(String key, String name, long value, long time) {
		Jedis jedis = cache.getJedis();
		try {
			setTime(jedis, key, time);
			return jedis.hincrBy(key, name, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public long decr(String key, String name, long value, long time) {
		return incr(key, name, -value, time);
	}

	@Override
	public void deleteCount(String key, String name) {
		cache.hdel(key, name);
	}

}
