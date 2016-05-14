package dataplatform.cache;

import java.util.Map;

import com.google.common.collect.Maps;

import dataplatform.cache.lazy.ILazyCache;
import dataplatform.cache.lazy.LazyCache;
import dataplatform.cache.object.ObjectCache;
import dataplatform.cache.redis.JedisResources;
import dataplatform.cache.redis.bytes.RedisBytesPoolCache;
import dataplatform.coder.bytes.ByteCoders;
import dataplatform.coder.bytes.IBytesCoder;

public class DefaultCacheManager implements ICacheManager {
	
	private final ICache<byte[], byte[], byte[]> cache;
	
	private final IBytesCoder bytesCoder;
	
	@SuppressWarnings("rawtypes")
	private final Map<String, ICache> caches;
	
	public DefaultCacheManager(String address, int maxTotal, int maxIdle, long waitTime) {
		cache = new RedisBytesPoolCache(new JedisResources(address, maxTotal, maxIdle, waitTime));
		bytesCoder = ByteCoders.newProtoStuffCoder();
		caches = Maps.newHashMap();
	}

	@Override
	public <K, F, V> ICache<K, F, V> getCache(Class<K> kclz, Class<F> fclz, Class<V> vclz) {
		return getCacheOnCreate(kclz, fclz, vclz);
	}

	@Override
	public <F, V> ILazyCache<F, V> getLazyCache(Class<F> fclz, Class<V> vclz, String preKey) {
		return new LazyCache<>(getCache(String.class, fclz, vclz), preKey);
	}
	
	protected <K, F, V> ICache<K, F, V> getCacheOnCreate(Class<K> kclz, Class<F> fclz, Class<V> vclz) {
		StringBuffer keyBulider = new StringBuffer();
		keyBulider.append(kclz.getName()).append(fclz.getName()).append(vclz.getName());
		String key = keyBulider.toString();
		@SuppressWarnings("unchecked")
		ICache<K, F, V> cache = caches.get(key);
		if (cache == null) {
			cache = new ObjectCache<>(this.cache, bytesCoder, kclz, fclz, vclz);
			caches.put(key, cache);
		}
		return cache;
	}

}
