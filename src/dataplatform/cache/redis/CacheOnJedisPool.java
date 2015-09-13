package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import dataplatform.cache.sequence.ICounter;

public class CacheOnJedisPool extends CacheOnJedis<Jedis, Jedis> implements ICounter {
	
	private final JedisPool pool;
	
	public CacheOnJedisPool(String address, int maxTotal, int maxIdle, long waitTime) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setMaxWaitMillis(waitTime);
		String[] infos = address.split(":");
		pool = new JedisPool(config, infos[0], Integer.parseInt(infos[1]));
	}

	@Override
	public void mSet(Map<Serializable, Serializable> map) {
		Jedis jedis = getJedisCommands();
		try {
			int mapSize = map.size();
			byte[][] keysvalues = new byte[mapSize << 1][];
			int index = 0;
			for (Serializable key : map.keySet()) {
				keysvalues[index << 1] = serializable(key);
				keysvalues[(index << 1) + 1] = serializable(map.get(key));
				index++;
			}
			jedis.mset(keysvalues);
		} catch (Exception e) {
			log.error("", e);
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public void mDel(Serializable... keys) {
		Jedis jedis = getJedisCommands();
		try {
			byte[][] keyBytes = serializable(keys);
			for (byte[] key : keyBytes) {
				jedis.del(key);
			}
		} catch (Exception e) {
			log.error("", e);
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public List<Serializable> mGet(Serializable... keys) {
		Jedis jedis = getJedisCommands();
		try {
			byte[][] keyBytes = serializable(keys);
			List<byte[]> list = jedis.mget(keyBytes);
			return deserializable(list);
		} catch (Exception e) {
			log.error("", e);
			throw new RedisException(e);
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public void clear() {
		Jedis jedis = getJedisCommands();
		try {
			jedis.flushAll();
			log.info("clear {}.");
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public Jedis getBinaryJedisCommands() {
		return pool.getResource();
	}

	@Override
	public void useFinishB(Jedis jedis) {
		jedis.close();
	}

	@Override
	public Jedis getJedisCommands() {
		return pool.getResource();
	}

	@Override
	public long get(String key) {
		Jedis jedis = getJedisCommands();
		try {
			return Long.parseLong(jedis.get(key));
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public long incr(String key, long value) {
		Jedis jedis = getJedisCommands();
		try {
			return jedis.incrBy(key, value);
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public long decr(String key, long value) {
		Jedis jedis = getJedisCommands();
		try {
			return jedis.decrBy(key, value);
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public void delete(String key) {
		Jedis jedis = getJedisCommands();
		try {
			jedis.del(key);
		} finally {
			useFinishB(jedis);
		}
	}

	@Override
	public void useFinishJ(Jedis jedis) {
		jedis.close();
	}

}
