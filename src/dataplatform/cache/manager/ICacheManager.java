package dataplatform.cache.manager;

import dataplatform.cache.ICache;

public interface ICacheManager {
	
	<K, F, V> ICache<K, F, V> createCache();
	
	<K, F, V> ICache<K, F, V> getCache(Class<K> kclz, Class<F> fclz, Class<V> vclz);

}
