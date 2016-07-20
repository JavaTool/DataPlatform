package dataplatform.cache.object;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import dataplatform.cache.ICacheKey;
import dataplatform.coder.bytes.IStreamCoder;

public class ObjectCacheKey<T> extends StreamCoderCache implements ICacheKey<T> {
	
	private final ICacheKey<byte[]> cacheKey;
	
	public ObjectCacheKey(ICacheKey<byte[]> cacheKey, IStreamCoder streamCoder) {
		super(streamCoder);
		this.cacheKey = cacheKey;
	}

	@Override
	public void delete(Object... keys) {
		cacheKey.delete(serializa(keys));
	}

	@Override
	public boolean exists(T key) {
		return cacheKey.exists(serializa(key));
	}

	@Override
	public void expire(T key, long time, TimeUnit timeUnit) {
		cacheKey.expire(serializa(key), time, timeUnit);
	}

	@Override
	public void expireat(T key, long timestamp) {
		cacheKey.expireat(serializa(key), timestamp);
	}

	@Override
	public Set<T> keys(T pattern) {
		return deserializa(cacheKey.keys(pattern.toString().getBytes()));
	}

	@Override
	public void persist(T key) {
		cacheKey.persist(serializa(key));
	}

	@Override
	public long ttl(T key) {
		return cacheKey.ttl(serializa(key));
	}

	@Override
	public void rename(T key, T newkey) {
		cacheKey.rename(serializa(key), serializa(newkey));
	}

	@Override
	public ValueType type(T key) {
		return cacheKey.type(serializa(key));
	}

}
