package dataplatform.cache.sequence.impl;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.ShardedJedis;
import dataplatform.cache.redis.CacheOnShardedRedis;
import dataplatform.cache.sequence.IInstanceIdManager;
import dataplatform.util.SerializaUtil;

public class ShardedRedisIdManager extends CacheOnShardedRedis implements IInstanceIdManager {
	
	protected Map<String, byte[]> keys;

	public ShardedRedisIdManager(String redisHostContent, int max) {
		super(redisHostContent, max);
		keys = new HashMap<String, byte[]>();
	}

	@Override
	public void create(String name, int baseValue) {
		ShardedJedis sharded = jedis.getResource();
		try {
			byte[] key = SerializaUtil.serializable(name);
			if (!sharded.exists(key)) {
				sharded.set(key, SerializaUtil.serializable(baseValue));
				keys.put(name, key);
			}
		} catch (Exception e) {
			log.error("", e);
		} finally {
			jedis.returnResourceObject(sharded);
		}
	}

	@Override
	public int next(String name) {
		ShardedJedis sharded = jedis.getResource();
		try {
			return sharded.incr(keys.get(name)).intValue();
		} finally {
			jedis.returnResourceObject(sharded);
		}
	}

}
