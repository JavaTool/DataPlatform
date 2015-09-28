package dataplatform.cache;

import java.util.Map;

public class HashCache<V extends IHashCachedObject> {
	
	/**键前缀*/
	protected final String preKey;
	/**缓存器*/
	protected final ICache cache;
	
	public HashCache(String preKey, ICache cache) {
		this.preKey = preKey;
		this.cache = cache;
		cache.registerCache(preKey, IHashCachedObject.class, true, null);
	}
	
	public void cache(String key, V hashObject) {
		cache.hset(makeKey(key), hashObject.getHashName(), hashObject);
	}
	
	@SuppressWarnings("unchecked")
	public V get(String key, String name) {
		return (V) cache.hget(makeKey(key), name);
	}
	
	public void delete(String key, String name) {
		cache.hdel(makeKey(key), name);
	}
	
	public void delete(String key, V hashObject) {
		delete(makeKey(key), hashObject.getHashName());
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, V> getAll(String key) {
		return (Map<String, V>) cache.hGetAll(makeKey(key));
	}
	
	protected String makeKey(String key) {
		return key == null || key.length() == 0 ? preKey : (preKey + "_" + key);
	}

}
