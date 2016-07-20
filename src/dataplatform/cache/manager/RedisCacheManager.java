package dataplatform.cache.manager;

import java.util.Map;

import com.google.common.collect.Maps;

import dataplatform.cache.ICache;
import dataplatform.cache.object.ObjectCache;
import dataplatform.cache.redis.IJedisReources;
import dataplatform.cache.redis.JedisPoolResources;
import dataplatform.cache.redis.bytes.RedisBytesPoolCache;
import dataplatform.coder.bytes.IStreamCoder;
import dataplatform.coder.bytes.StreamCoders;

public class RedisCacheManager implements ICacheManager {
	
	private final ICache<byte[], byte[], byte[]> cache;
	
	private final IStreamCoder streamCoder;
	
	@SuppressWarnings("rawtypes")
	private final Map<String, ICache> caches;
	
	private final IJedisReources jedisReources;
	
	public RedisCacheManager(String address, int maxTotal, int maxIdle, long waitTime) {
		jedisReources = new JedisPoolResources(address, maxTotal, maxIdle,waitTime);
		cache = new RedisBytesPoolCache(jedisReources);
		streamCoder = StreamCoders.newProtoStuffCoder();
		caches = Maps.newHashMap();
	}

	@Override
	public <K, F, V> ICache<K, F, V> getCache(Class<K> kclz, Class<F> fclz, Class<V> vclz) {
		return getCacheOnCreate(kclz, fclz, vclz);
	}
	
	protected <K, F, V> ICache<K, F, V> getCacheOnCreate(Class<K> kclz, Class<F> fclz, Class<V> vclz) {
		String key = makeClassString(kclz, fclz, vclz);
		@SuppressWarnings("unchecked")
		ICache<K, F, V> cache = caches.get(key);
		if (cache == null) {
			cache = createCache();
			caches.put(key, cache);
		}
		return cache;
	}
	
	protected <K, F, V> String makeClassString(Class<K> kclz, Class<F> fclz, Class<V> vclz) {
		StringBuffer keyBulider = new StringBuffer();
		keyBulider.append(kclz.getName()).append(fclz.getName()).append(vclz.getName());
		return keyBulider.toString();
	}

	@Override
	public <K, F, V> ICache<K, F, V> createCache() {
		return new ObjectCache<>(cache, streamCoder);
	}
	
	public IJedisReources getJedisReources() {
		return jedisReources;
	}

}
