package dataplatform.cache.object;

import dataplatform.cache.ICacheList;
import dataplatform.coder.bytes.IBytesCoder;

public class ObjectCacheList<K, V> extends StreamCoderCache implements ICacheList<K, V> {
	
	private final ICacheList<byte[], byte[]> cacheValue;
	
	private final Class<V> vclz;

	public ObjectCacheList(ICacheList<byte[], byte[]> cacheValue, IBytesCoder bytesCoder, Class<V> vclz) {
		super(bytesCoder);
		this.cacheValue = cacheValue;
		this.vclz = vclz;
	}

	@Override
	public V headPop(K key) {
		return deserializa(cacheValue.headPop(serializa(key)), vclz);
	}

	@Override
	public void tailPush(K key, Object... objects) {
		cacheValue.tailPush(serializa(key), serializa(objects));
	}

	@Override
	public V get(K key, long index) {
		return deserializa(cacheValue.get(serializa(key), index), vclz);
	}

	@Override
	public long size(K key) {
		return cacheValue.size(serializa(key));
	}

	@Override
	public void trim(K key, long start, long end) {
		cacheValue.trim(serializa(key), start, end);
	}

}
