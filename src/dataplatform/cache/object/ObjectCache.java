package dataplatform.cache.object;

import dataplatform.cache.ICache;
import dataplatform.cache.ICacheHash;
import dataplatform.cache.ICacheKey;
import dataplatform.cache.ICacheList;
import dataplatform.cache.ICacheValue;
import dataplatform.coder.bytes.IStreamCoder;

public class ObjectCache<K, F, V> extends StreamCoderCache implements ICache<K, F, V> {
	
	private final ICacheKey<K> cacheKey;
	
	private final ICacheHash<K, F, V> cacheHash;
	
	private final ICacheList<K, V> cacheList;
	
	private final ICacheValue<K, V> cacheValue;

	public ObjectCache(ICache<byte[], byte[], byte[]> cache, IStreamCoder streamCoder) {
		super(streamCoder);
		cacheKey = new ObjectCacheKey<>(cache.key(), streamCoder);
		cacheHash = new ObjectCacheHash<>(cache.hash(), streamCoder);
		cacheList = new ObjectCacheList<>(cache.list(), streamCoder);
		cacheValue = new ObjectCacheValue<>(cache.value(), streamCoder);
	}

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
