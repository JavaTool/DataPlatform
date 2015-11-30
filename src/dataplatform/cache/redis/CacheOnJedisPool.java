package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

import dataplatform.cache.ICacheUnit;
import dataplatform.coder.bytes.IBytesCoder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class CacheOnJedisPool extends CacheOnJedis {
	
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
	public void mSet(Map<String, Object> map) {
		Jedis jedis = getJedis();
		int mapSize = map.size();
		Set<String> keySet = map.keySet();
		String[] keys = keySet.toArray(new String[mapSize]);
		ICacheUnit cacheUnit = checkMCache(keys);
		try {
			if (cacheUnit == null) {
				String[] array = new String[mapSize << 1];
				for (int i = 0;i < keys.length;i++) {
					array[i << 1] = keys[i];
					array[(i << 1) + 1] = map.get(keys[i]).toString();
				}
				jedis.mset(array);
			} else {
				IBytesCoder streamCoder = cacheUnit.getStreamCoder();
				byte[][] array = new byte[mapSize << 1][];
				for (int i = 0;i < keys.length;i++) {
					array[i << 1] = serializable(keys[i]);
					array[(i << 1) + 1] = streamCoder.write(map.get(keys[i]));
				}
				jedis.mset(array);
			}
		} catch (Exception e) {
			log.error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void mDel(String... keys) {
		delExecutor.exec(null, null, checkMCache(keys), null, keys);
	}

	@Override
	public List<Object> mGet(String... keys) {
		List<Object> list = Lists.newArrayListWithCapacity(keys.length);
		getExecutor.exec(null, null, checkMCache(keys), list, keys);
		return list;
	}
	
	protected class GetExecutorEx extends GetExecutor {

		@Override
		protected Serializable execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) {
			return collection.addAll(jedis.mget(names));
		}

		@Override
		protected Serializable execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IBytesCoder streamCoder, byte[]... names) throws Exception {
			for (byte[] datas : jedis.mget(names)) {
				collection.add(streamCoder.read(datas));
			}
			return null;
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
	public void useFinish(Jedis jedis) {
		// quit
		try {
			jedis.quit();
		} catch (Exception e) {
			log.error("", e);
		}
		// close
		try {
			jedis.close();
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
