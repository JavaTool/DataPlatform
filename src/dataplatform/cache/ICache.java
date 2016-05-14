package dataplatform.cache;

public interface ICache<K, F, V> {
	
	ICacheKey<K> key();
	
	ICacheHash<K, F, V> hash();
	
	ICacheList<K, V> list();
	
	ICacheValue<K, V> value();

}
