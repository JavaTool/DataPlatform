package dataplatform.sequence.impl;

import java.util.Map;

import com.google.common.collect.Maps;

import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;
import dataplatform.sequence.IInstanceIdMaker;
import dataplatform.sequence.IInstanceIdManager;

/**
 * Redis-id管理器
 * @author	fuhuiyuan
 */
public class RedisIdManager extends ExistsJedisReources implements IInstanceIdManager {
	
	/**id生成器集合*/
	protected Map<String, IInstanceIdMaker> idMakers;

	public RedisIdManager(IJedisReources jedisReources) {
		setResouces(jedisReources);
		idMakers = Maps.newHashMap();
	}

	@Override
	public void create(String name, int baseValue) {
		if (!idMakers.containsKey(name)) {
			try {
				idMakers.put(name, new RedisIdMaker(name, this, baseValue));
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
