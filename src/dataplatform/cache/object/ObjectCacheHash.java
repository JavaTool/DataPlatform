package dataplatform.cache.object;

import java.util.List;
import java.util.Map;

import dataplatform.cache.ICacheHash;
import dataplatform.coder.bytes.IBytesCoder;

public class ObjectCacheHash<T, F, V> extends StreamCoderCache implements ICacheHash<T, F, V> {
	
	private final ICacheHash<byte[], byte[], byte[]> cacheHash;
	
	private final Class<F> fclz;
	
	private final Class<V> vclz;

	public ObjectCacheHash(ICacheHash<byte[], byte[], byte[]> cacheHash, IBytesCoder bytesCoder, Class<F> fclz, Class<V> vclz) {
		super(bytesCoder);
		this.cacheHash = cacheHash;
		this.fclz = fclz;
		this.vclz = vclz;
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
		return deserializa(cacheHash.get(serializa(key), serializa(field)), vclz);
	}

	@Override
	public List<V> get(T key, Object... fields) {
		return deserializa(cacheHash.get(serializa(key), serializa(fields)), vclz);
	}

	@Override
	public Map<F, V> getAll(T key) {
		return deserializa(cacheHash.getAll(serializa(key)), fclz, vclz);
	}

	@Override
	public List<F> fields(T key) {
		return deserializa(cacheHash.fields(serializa(key)), fclz);
	}

	@Override
	public long size(T key) {
		return cacheHash.size(serializa(key));
	}

	@Override
	public void set(T key, Map<F, V> map) {
		cacheHash.set(serializa(key), serializa(map));
	}

	@Override
	public void set(T key, F field, V value) {
		cacheHash.set(serializa(key), serializa(field), serializa(value));
	}

	@Override
	public List<V> values(T key) {
		return deserializa(cacheHash.values(serializa(key)), vclz);
	}

}
