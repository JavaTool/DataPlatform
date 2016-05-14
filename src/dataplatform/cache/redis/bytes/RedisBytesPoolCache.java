package dataplatform.cache.redis.bytes;

import dataplatform.cache.ICacheHash;
import dataplatform.cache.ICacheKey;
import dataplatform.cache.ICacheList;
import dataplatform.cache.ICacheValue;
import dataplatform.cache.redis.IJedisReources;
import dataplatform.cache.redis.RedisPoolCache;

public class RedisBytesPoolCache extends RedisPoolCache<byte[], byte[], byte[]> {

	public RedisBytesPoolCache(IJedisReources jedisReources) {
		super(jedisReources);
	}

	@Override
	protected ICacheKey<byte[]> createCacheKey(IJedisReources jedisReources) {
		return new RedisBytesKey(jedisReources);
	}

	@Override
	protected ICacheHash<byte[], byte[], byte[]> createCacheHash(IJedisReources jedisReources) {
		return new RedisBytesHash(jedisReources);
	}

	@Override
	protected ICacheList<byte[], byte[]> createCacheList(IJedisReources jedisReources) {
		return new RedisBytesList(jedisReources);
	}

	@Override
	protected ICacheValue<byte[], byte[]> createCacheValue(IJedisReources jedisReources) {
		return new RedisBytesValue(jedisReources);
	}

}
