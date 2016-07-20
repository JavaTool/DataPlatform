package dataplatform.cache.redis.string;

import dataplatform.cache.ICacheHash;
import dataplatform.cache.ICacheKey;
import dataplatform.cache.ICacheList;
import dataplatform.cache.ICacheValue;
import dataplatform.cache.redis.IJedisReources;
import dataplatform.cache.redis.RedisPoolCache;

public class RedisStringPoolCache extends RedisPoolCache<String, String, String> {
	
	public RedisStringPoolCache(IJedisReources jedisReources) {
		super(jedisReources);
	}

	@Override
	protected ICacheKey<String> createCacheKey(IJedisReources jedisReources) {
		return new RedisStringKey(jedisReources);
	}

	@Override
	protected ICacheHash<String, String, String> createCacheHash(IJedisReources jedisReources) {
		return new RedisStringHash(jedisReources);
	}

	@Override
	protected ICacheList<String, String> createCacheList(IJedisReources jedisReources) {
		return new RedisStringList(jedisReources);
	}

	@Override
	protected ICacheValue<String, String> createCacheValue(IJedisReources jedisReources) {
		return new RedisStringValue(jedisReources);
	}

}
