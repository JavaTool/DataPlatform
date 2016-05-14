package dataplatform.cache.object;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import dataplatform.cache.ICacheValue;
import dataplatform.coder.bytes.IBytesCoder;

public class ObjectCacheValue<K, V> extends StreamCoderCache implements ICacheValue<K, V> {
	
	private final ICacheValue<byte[], byte[]> cacheValue;
	
	private final Class<V> vclz;
	
	public ObjectCacheValue(ICacheValue<byte[], byte[]> cacheValue, IBytesCoder bytesCoder, Class<V> vclz) {
		super(bytesCoder);
		this.cacheValue = cacheValue;
		this.vclz = vclz;
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
		return deserializa(cacheValue.get(serializa(key)), vclz);
	}

	@Override
	public List<V> get(Object... keys) {
		return deserializa(cacheValue.get(serializa(keys)), vclz);
	}

	@Override
	public long incr(K key, long increment) {
		return cacheValue.incr(serializa(key), increment);
	}

	@Override
	public void set(K key, V value) {
		cacheValue.set(serializa(key), serializa(value));
	}

	@Override
	public void set(K key, V value, boolean exists, long time, TimeUnit timeUnit) {
		cacheValue.set(serializa(key), serializa(value), exists, time, timeUnit);
	}

	@Override
	public void set(Map<K, V> map) {
		cacheValue.set(serializa(map));
	}

}
