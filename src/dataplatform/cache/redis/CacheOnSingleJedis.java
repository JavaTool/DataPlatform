package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import dataplatform.coder.bytes.IBytesCoder;
import redis.clients.jedis.Jedis;

/**
 * Redis缓存器
 * @author 	fuhuiyuan
 */
public class CacheOnSingleJedis extends CacheOnJedis {
	
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
	public void mDel(String... keys) {
		delExecutor.exec(null, null, null, null, keys);
	}

	@Override
	public List<Object> mGet(String... keys) {
		List<Object> list = Lists.newArrayListWithCapacity(keys.length);
		getExecutor.exec(null, null, checkMCache(keys), list, keys);
		return list;
	}
	
	protected class GetExecutorEx extends GetExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) {
			return collection.addAll(jedis.mget(names));
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IBytesCoder streamCoder, byte[]... names) throws Exception {
			for (byte[] datas : jedis.mget(names)) {
				collection.add((Serializable) streamCoder.read(datas));
			}
			return null;
		}
		
	}

	@Override
	public void mSet(Map<String, Object> map) {
		try {
			int mapSize = map.size();
			byte[][] keysvalues = new byte[mapSize << 1][];
			int index = 0;
			for (Serializable key : map.keySet()) {
				keysvalues[index << 1] = serializable(key);
				keysvalues[(index << 1) + 1] = serializable((Serializable) map.get(key)); // TODO Object
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
