package dataplatform.cache;

import dataplatform.cache.lazy.ILazyCache;

public interface ICacheManager {
	
	<K, F, V> ICache<K, F, V> getCache(Class<K> kclz, Class<F> fclz, Class<V> vclz);
	
	<F, V> ILazyCache<F, V> getLazyCache(Class<F> fclz, Class<V> vclz, String preKey);

}
