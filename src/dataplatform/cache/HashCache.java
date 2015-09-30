package dataplatform.cache;

import java.util.Map;

import com.google.common.collect.Maps;

public class HashCache<V extends IHashCachedObject> {
	
	/**键前缀*/
	protected final String preKey;
	/**缓存器*/
	protected final ICache cache;
	
	protected final IForEach<V> mSetForeach;
	
	protected final IForEach<V> mGetForeach;
	
	protected final Class<V> clz;
	
	public HashCache(String preKey, ICache cache, Class<V> clz) {
		this.preKey = preKey;
		this.cache = cache;
		this.clz = clz;
		mSetForeach = createMSetForEach();
		mGetForeach = createMGetForEach();
		cache.registerCache(preKey, clz, true, null);
	}
	
	public void cache(Object key, V hashObject) {
		cache(key.toString(), hashObject);;
	}
	
	public void cache(String key, V hashObject) {
		cache.hset(checkAndMakeKey(key), hashObject.getHashName(), hashObject);
	}
	
	public void cache(String key, Map<?, V> hashObjects, IForEach<V> foreach) {
		if (hashObjects.size() > 0) {
			Map<String, Object> map = Maps.newHashMap();
			for (V hashObject : hashObjects.values()) {
				map.put(hashObject.getHashName(), hashObject);
				if (foreach != null) {
					foreach.each(key, hashObject);
				}
			}
			cache.hmSet(checkAndMakeKey(key), map);
		}
	}
	
	public void cache(String key, Map<?, V> hashObjects, boolean foreach) {
		cache(key, hashObjects, foreach ? this.mSetForeach : null);
	}
	
	@SuppressWarnings("unchecked")
	public V get(String key, String name) {
		return (V) cache.hget(makeKey(key), name);
	}

	public V get(Object key, Object name) {
		return get(key.toString(), name.toString());
	}
	
	public void delete(String key) {
		cache.del(makeKey(key));
	}
	
	public void delete(String key, String name) {
		cache.hdel(makeKey(key), name);
	}
	
	public void delete(String key, V hashObject) {
		delete(makeKey(key), hashObject.getHashName());
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, V> getAll(String key, IForEach<V> foreach) {
		Map<String, V> all = (Map<String, V>) cache.hGetAll(makeKey(key));
		if (foreach != null) {
			for (V v : all.values()) {
				foreach.each(key, v);
			}
		}
		return all;
	}

	public Map<String, V> getAll(String key, boolean foreach) {
		return getAll(key, foreach ? mGetForeach : null);
	}
	
	public static interface IForEach<V extends IHashCachedObject> {
		
		void each(String key, V hashObject);
		
	}
	
	protected String makeKey(String key) {
		return key == null || key.length() == 0 ? preKey : (preKey + "_" + key);
	}
	
	protected String checkAndMakeKey(String key) {
		String cacheKey = makeKey(key);
		if (!cache.containsCacheKey(cacheKey)) {
			cache.registerCache(cacheKey, clz, true, null);
		}
		return cacheKey;
	}
	
	protected IForEach<V> createMSetForEach() {
		return null;
	}
	
	protected IForEach<V> createMGetForEach() {
		return null;
	}

}
