package dataplatform.cache.object;

import java.util.List;
import java.util.Map;
import java.util.Set;

import dataplatform.cache.ICacheHash;
import dataplatform.coder.bytes.IStreamCoder;

public class ObjectCacheHash<T, F, V> extends StreamCoderCache implements ICacheHash<T, F, V> {
	
	private final ICacheHash<byte[], byte[], byte[]> cacheHash;

	public ObjectCacheHash(ICacheHash<byte[], byte[], byte[]> cacheHash, IStreamCoder streamCoder) {
		super(streamCoder);
		this.cacheHash = cacheHash;
	}

	@Override
	public void remove(T key, Object... fields) {
		cacheHash.remove(serializa(key), serializa(fields));
	}

	@Override
	public boolean contains(T key, F field) {
		return cacheHash.contains(serializa(key), serializa(field));
	}

	@Override
	public V get(T key, F field) {
		return deserializa(cacheHash.get(serializa(key), serializa(field)));
	}

	@Override
	public List<V> multiGet(T key, Object... fields) {
		return deserializa(cacheHash.multiGet(serializa(key), serializa(fields)));
	}

	@Override
	public Map<F, V> getAll(T key) {
		return deserializa(cacheHash.getAll(serializa(key)));
	}

	@Override
	public Set<F> fields(T key) {
		return deserializa(cacheHash.fields(serializa(key)));
	}

	@Override
	public long size(T key) {
		return cacheHash.size(serializa(key));
	}

	@Override
	public void multiSet(T key, Map<F, V> map) {
		cacheHash.multiSet(serializa(key), serializa(map));
	}

	@Override
	public void set(T key, F field, V value) {
		cacheHash.set(serializa(key), serializa(field), serializa(value));
	}

	@Override
	public List<V> values(T key) {
		return deserializa(cacheHash.values(serializa(key)));
	}

}
