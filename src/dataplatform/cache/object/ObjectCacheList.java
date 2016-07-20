package dataplatform.cache.object;

import dataplatform.cache.ICacheList;
import dataplatform.coder.bytes.IStreamCoder;

public class ObjectCacheList<K, V> extends StreamCoderCache implements ICacheList<K, V> {
	
	private final ICacheList<byte[], byte[]> cacheValue;

	public ObjectCacheList(ICacheList<byte[], byte[]> cacheValue, IStreamCoder streamCoder) {
		super(streamCoder);
		this.cacheValue = cacheValue;
	}

	@Override
	public V headPop(K key) {
		return deserializa(cacheValue.headPop(serializa(key)));
	}

	@Override
	public void tailPush(K key, Object... objects) {
		cacheValue.tailPush(serializa(key), serializa(objects));
	}

	@Override
	public V get(K key, long index) {
		return deserializa(cacheValue.get(serializa(key), index));
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
