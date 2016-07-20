package dataplatform.cache.object;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import dataplatform.cache.ICacheValue;
import dataplatform.coder.bytes.IStreamCoder;

public class ObjectCacheValue<K, V> extends StreamCoderCache implements ICacheValue<K, V> {
	
	private final ICacheValue<byte[], byte[]> cacheValue;
	
	public ObjectCacheValue(ICacheValue<byte[], byte[]> cacheValue, IStreamCoder streamCoder) {
		super(streamCoder);
		this.cacheValue = cacheValue;
	}

	@Override
	public void append(K key, V value) {
		cacheValue.append(serializa(key), serializa(value));
	}

	@Override
	public long decr(K key, long decrement) {
		return cacheValue.decr(serializa(key), decrement);
	}

	@Override
	public V get(K key) {
		return deserializa(cacheValue.get(serializa(key)));
	}

	@Override
	public List<V> multiGet(Object... keys) {
		return deserializa(cacheValue.multiGet(serializa(keys)));
	}

	@Override
	public long incr(K key, long increment) {
		return cacheValue.incr(serializa(key), increment);
	}

	@Override
	public void set(K key, V value) {
		deserializa(serializa(key));
		cacheValue.set(serializa(key), serializa(value));
	}

	@Override
	public boolean xSet(K key, V value, boolean exists, long time, TimeUnit timeUnit) {
		return cacheValue.xSet(serializa(key), serializa(value), exists, time, timeUnit);
	}

	@Override
	public void multiSet(Map<K, V> map) {
		cacheValue.multiSet(serializa(map));
	}

}
