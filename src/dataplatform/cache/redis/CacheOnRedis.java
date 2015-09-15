package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import dataplatform.cache.ICache;
import dataplatform.cache.sequence.ICounter;

/**
 * Redis缓存器
 * @author 	fuhuiyuan
 */
public class CacheOnRedis extends CacheOnJedis<Jedis, Jedis> implements ICache, ICounter {
	
	protected static final Logger log = LoggerFactory.getLogger(CacheOnRedis.class);
	
	protected Jedis jedis;
	/**Redis信息*/
	protected RedisHost host;
	
	public CacheOnRedis(String redisHostContent) {
		String[] hostInfos = redisHostContent.split(":");
		host = new RedisHost(hostInfos[0], Integer.parseInt(hostInfos[1]));
		jedis = new Jedis(host.getHost(), host.getPort());
		log.info("Redis connect, {}.", jedis.toString());
	}

	@Override
	public long getCount(Serializable key) {
		try {
			String vaule = (String) deserializable(jedis.get(serializable(key)));
			return Long.parseLong(vaule == null ? "0" : vaule);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		}
	}

	@Override
	public long incr(Serializable key, long value) {
		try {
			return jedis.incrBy(serializable(key), value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		}
	}

	@Override
	public long decr(Serializable key, long value) {
		try {
			return jedis.decrBy(serializable(key), value);
		} catch (Exception e) {
			log.error("", e);
			return 0L;
		}
	}

	@Override
	public void deleteCount(Serializable key) {
		del(key);
	}

	@Override
	public Jedis getBinaryJedisCommands() {
		return jedis;
	}

	@Override
	public void mDel(Serializable... keys) {
		try {
			byte[][] keyBytes = serializable(keys);
			for (byte[] key : keyBytes) {
				getBinaryJedisCommands().del(key);
			}
		} catch (Exception e) {
			log.error("", e);
		}
	}

	@Override
	public List<Serializable> mGet(Serializable... keys) {
		try {
			byte[][] keyBytes = serializable(keys);
			List<byte[]> list = getBinaryJedisCommands().mget(keyBytes);
			return deserializable(list);
		} catch (Exception e) {
			log.error("", e);
			throw new RedisException(e);
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
			getBinaryJedisCommands().mset(keysvalues);
		} catch (Exception e) {
			log.error("", e);
		}
	}

	@Override
	public void clear() {
		getBinaryJedisCommands().flushAll();
		log.info("clear {}.", host);
	}

	@Deprecated
	@Override
	public void useFinishB(Jedis jedis) {}

	@Override
	public Jedis getJedisCommands() {
		return jedis;
	}

	@Deprecated
	@Override
	public void useFinishJ(Jedis jedis) {}

	@Override
	public void shutdown() {
		jedis.close();
	}

}
