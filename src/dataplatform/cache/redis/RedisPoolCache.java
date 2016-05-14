package dataplatform.cache.redis;

import dataplatform.cache.ICache;
import dataplatform.cache.ICacheHash;
import dataplatform.cache.ICacheKey;
import dataplatform.cache.ICacheList;
import dataplatform.cache.ICacheValue;

public abstract class RedisPoolCache<K, F, V> implements ICache<K, F, V> {
	
	private final ICacheKey<K> cacheKey;
	
	private final ICacheHash<K, F, V> cacheHash;
	
	private final ICacheList<K, V> cacheList;
	
	private final ICacheValue<K, V> cacheValue;
	
	public RedisPoolCache(IJedisReources jedisReources) {
		cacheKey = createCacheKey(jedisReources);
		cacheHash = createCacheHash(jedisReources);
		cacheList = createCacheList(jedisReources);
		cacheValue = createCacheValue(jedisReources);
	}
	
	protected abstract ICacheKey<K> createCacheKey(IJedisReources jedisReources);
	
	protected abstract ICacheHash<K, F, V> createCacheHash(IJedisReources jedisReources);
	
	protected abstract ICacheList<K, V> createCacheList(IJedisReources jedisReources);
	
	protected abstract ICacheValue<K, V> createCacheValue(IJedisReources jedisReources);

	@Override
	public ICacheKey<K> key() {
		return cacheKey;
	}

	@Override
	public ICacheHash<K, F, V> hash() {
		return cacheHash;
	}

	@Override
	public ICacheList<K, V> list() {
		return cacheList;
	}

	@Override
	public ICacheValue<K, V> value() {
		return cacheValue;
	}

}
