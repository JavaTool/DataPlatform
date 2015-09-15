package dataplatform.cache.sequence.impl;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.sequence.ICounter;
import dataplatform.util.SerializaUtil;

public class RedisCounter<B extends BinaryJedisCommands, J extends JedisCommands> implements ICounter {
	
	protected static final Logger log = LoggerFactory.getLogger(RedisCounter.class);
	
	private final CacheOnJedis<B, J> cache;
	
	public RedisCounter(CacheOnJedis<B, J> cache) {
		this.cache = cache;
	}

	@Override
	public long getCount(Serializable key) {
		J jedis = cache.getJedisCommands();
		try {
			String vaule = (String) cache.get(key);
			return Long.parseLong(vaule == null ? "0" : vaule);
		} finally {
			cache.useFinishJ(jedis);
		}
	}
	
	/**
	 * 序列化
	 * @param 	object
	 * 			被序列化的对象
	 * @return	序列化结果
	 * @throws 	Exception
	 */
	protected static byte[] serializable(Serializable object) throws Exception {
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
	public long incr(Serializable key, long value) {
		B jedis = cache.getBinaryJedisCommands();
		try {
			return jedis.incrBy(serializable(key), value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			cache.useFinishB(jedis);
		}
	}

	@Override
	public long decr(Serializable key, long value) {
		B jedis = cache.getBinaryJedisCommands();
		try {
			return jedis.decrBy(serializable(key), value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			cache.useFinishB(jedis);
		}
	}

	@Override
	public void deleteCount(Serializable key) {
		cache.del(key);
	}

}
