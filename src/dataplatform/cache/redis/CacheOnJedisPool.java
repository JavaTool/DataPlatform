package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.google.common.collect.Lists;

import dataplatform.cache.sequence.ICounter;

public class CacheOnJedisPool extends CacheOnJedis implements ICounter {
	
	private final JedisPool pool;
	
	public CacheOnJedisPool(String address, int maxTotal, int maxIdle, long waitTime) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setMaxWaitMillis(waitTime);
		String[] infos = address.split(":");
		pool = new JedisPool(config, infos[0], Integer.parseInt(infos[1]));
		pool.getResource();
	}

	@Override
	public void mSet(Map<Serializable, Serializable> map) {
		int mapSize = map.size();
		Serializable[] array = new Serializable[mapSize << 1];
		int index = 0;
		for (Serializable key : map.keySet()) {
			array[index << 1] = key;
			array[(index << 1) + 1] = map.get(key);
			index++;
		}
		setExecutor.exec(null, null, checkMCache(array), null, array);
	}

	@Override
	protected ICacheExecutor createSetExecutor() {
		return new SetExecutorEx();
	}
	
	protected class SetExecutorEx extends SetExecutor {

		@Override
		protected Serializable execReids(Jedis jedis, String key, Map<String, String> map, Collection<Serializable> collection, String... names) {
			return jedis.mset(names);
		}

		@Override
		protected Serializable execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Serializable> collection, byte[]... names) {
			return jedis.mset(names);
		}
		
	}

	@Override
	public void mDel(Serializable... keys) {
		delExecutor.exec(null, null, checkMCache(keys), null, keys);
	}

	@Override
	public List<Serializable> mGet(Serializable... keys) {
		List<Serializable> list = Lists.newArrayListWithCapacity(keys.length);
		getExecutor.exec(null, null, checkMCache(keys), list, keys);
		return list;
	}
	
	protected class GetExecutorEx extends GetExecutor {

		@Override
		protected Serializable execReids(Jedis jedis, String key, Map<String, String> map, Collection<Serializable> collection, String... names) {
			return collection.addAll(jedis.mget(names));
		}

		@Override
		protected Serializable execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Serializable> collection, byte[]... names) {
			return collection.addAll(jedis.mget(names));
		}
		
	}

	@Override
	protected ICacheExecutor createGetExecutor() {
		return new GetExecutorEx();
	}

	@Override
	public void clear() {
		Jedis jedis = getJedis();
		try {
			jedis.flushAll();
			log.info("clear {}.");
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Jedis getJedis() {
		return pool.getResource();
	}

	@Override
	public long getCount(String key) {
		Jedis jedis = getJedis();
		try {
			String vaule = jedis.get(key);
			return Long.parseLong(vaule == null ? "0" : vaule);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long incr(String key, long value) {
		if (!cacheUnits.containsKey(key)) {
			cacheUnits.put(key, CacheUnitFactory.createCacheUnit(key, Long.class, true, null));
		}
		Jedis jedis = getJedis();
		try {
			return jedis.incrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long decr(String key, long value) {
		if (!cacheUnits.containsKey(key)) {
			cacheUnits.put(key, CacheUnitFactory.createCacheUnit(key, Long.class, true, null));
		}
		Jedis jedis = getJedis();
		try {
			return jedis.decrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void deleteCount(String key) {
		del(key);
	}

	@Override
	public void useFinish(Jedis jedis) {
		// close
		try {
			jedis.close();
		} catch (Exception e) {
			log.error("", e);
		}
		// quit
		try {
			jedis.quit();
		} catch (Exception e) {
			log.error("", e);
		}
		// disconnect
		try {
			jedis.disconnect();
		} catch (Exception e) {
			log.error("", e);
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();
		pool.close();
		pool.destroy();
	}

}
