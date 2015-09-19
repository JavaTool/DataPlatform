package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Lists;

import dataplatform.cache.sequence.ICounter;

/**
 * Redis缓存器
 * @author 	fuhuiyuan
 */
public class CacheOnSingleJedis extends CacheOnJedis implements ICounter {
	
	protected static final Logger log = LoggerFactory.getLogger(CacheOnSingleJedis.class);
	
	protected Jedis jedis;
	/**Redis信息*/
	protected RedisHost host;
	
	public CacheOnSingleJedis(String redisHostContent) {
		String[] hostInfos = redisHostContent.split(":");
		host = new RedisHost(hostInfos[0], Integer.parseInt(hostInfos[1]));
		jedis = new Jedis(host.getHost(), host.getPort());
		log.info("Redis connect, {}.", jedis.toString());
	}

	@Override
	public long getCount(String key) {
		try {
			String vaule = jedis.get(key);
			return Long.parseLong(vaule == null ? "0" : vaule);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		}
	}

	@Override
	public long incr(String key, long value) {
		try {
			return jedis.incrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		}
	}

	@Override
	public long decr(String key, long value) {
		try {
			return jedis.decrBy(key, value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		}
	}

	@Override
	public void deleteCount(String key) {
		del(key);
	}

	@Override
	public void mDel(Serializable... keys) {
		delExecutor.exec(null, null, null, null, keys);
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
	public void mSet(Map<Serializable, Serializable> map) {
		try {
			int mapSize = map.size();
			byte[][] keysvalues = new byte[mapSize << 1][];
			int index = 0;
			for (Serializable key : map.keySet()) {
				keysvalues[index << 1] = serializable(key);
				keysvalues[(index << 1) + 1] = serializable(map.get(key));
				index++;
			}
			getJedis().mset(keysvalues);
		} catch (Exception e) {
			log.error("", e);
		}
	}

	@Override
	public void clear() {
		getJedis().flushAll();
		log.info("clear {}.", host);
	}

	@Override
	public Jedis getJedis() {
		return jedis;
	}

	@Deprecated
	@Override
	public void useFinish(Jedis jedis) {}

	@Override
	public void shutdown() {
		super.shutdown();
		jedis.close();
	}

}