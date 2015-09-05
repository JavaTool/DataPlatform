package dataplatform.cache.sequence.impl;

import java.util.HashMap;
import java.util.Map;

import dataplatform.cache.redis.CacheOnRedis;
import dataplatform.cache.sequence.IInstanceIdMaker;
import dataplatform.cache.sequence.IInstanceIdManager;

/**
 * Redis-id管理器
 * @author	fuhuiyuan
 */
public class RedisIdManager extends CacheOnRedis implements IInstanceIdManager {
	
	/**id生成器集合*/
	protected Map<String, IInstanceIdMaker> idMakers;

	public RedisIdManager(String redisHostContent) {
		super(redisHostContent);
		idMakers = new HashMap<String, IInstanceIdMaker>();
	}

	@Override
	public void create(String name, int baseValue) {
		if (!idMakers.containsKey(name)) {
			try {
				idMakers.put(name, new RedisIdMaker(name, jedis, baseValue));
			} catch (Exception e) {
				log.error("", e);
			}
		}
	}

	@Override
	public int next(String name) {
		if (idMakers.containsKey(name)) {
			return idMakers.get(name).nextInstanceId();
		} else {
			log.error("Do not have {} id maker.", name);
			throw new NullPointerException();
		}
	}

}
