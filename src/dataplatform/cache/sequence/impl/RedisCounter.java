package dataplatform.cache.sequence.impl;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.sequence.ICounter;
import dataplatform.util.SerializaUtil;

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
			String vaule = (String) cache.get(key);
			return Long.parseLong(vaule == null ? "0" : vaule);
		} finally {
			cache.useFinish(jedis);
		}
	}
	
	/**
	 * 序列化
	 * @param 	object
	 * 			被序列化的对象
	 * @return	序列化结果
	 * @throws 	Exception
	 */
	protected static byte[] serializable(String object) throws Exception {
		return SerializaUtil.serializable(object);
	}
	
	/**
	 * 反序列化
	 * @param 	datas
	 * 			序列化内容
	 * @return	反序列化的对象
	 * @throws 	Exception
	 */
	protected static Serializable deserializable(byte[] datas) throws Exception {
		return SerializaUtil.deserializable(datas);
	}

	@Override
	public long incr(String key, long value) {
		Jedis jedis = cache.getJedis();
		try {
			return jedis.incrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public long decr(String key, long value) {
		Jedis jedis = cache.getJedis();
		try {
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

}
