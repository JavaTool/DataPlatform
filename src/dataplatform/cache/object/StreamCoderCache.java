package dataplatform.cache.object;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import dataplatform.coder.bytes.IStreamCoder;

abstract class StreamCoderCache {
	
	private final IStreamCoder streamCoder;
	
	public StreamCoderCache(IStreamCoder streamCoder) {
		this.streamCoder = streamCoder;
	}
	
	protected byte[] serializa(Object object) {
		try {
			return streamCoder.write(object);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Object[] serializa(Object... objects) {
		byte[][] ret = new byte[objects.length][];
		for (int i = 0;i < objects.length;i++) {
			ret[i] = serializa(objects[i]);
		}
		return ret;
	}
	
	protected <K, V> Map<byte[], byte[]> serializa(Map<K, V> map) {
		Map<byte[], byte[]> byteMap = newHashMap();
		map.forEach((k, v) -> {if (v != null) byteMap.put(serializa(k), serializa(v));});
		return byteMap;
	}
	
	protected <T> T deserializa(byte[] bytes) {
		try {
			return streamCoder.read(bytes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected <T> List<T> deserializa(List<byte[]> byteList) {
		List<T> ret = newArrayListWithCapacity(byteList.size());
		byteList.forEach(bytes -> ret.add(deserializa(bytes)));
		return ret;
	}
	
	protected <K, V> Map<K, V> deserializa(Map<byte[], byte[]> byteMap) {
		Map<K, V> ret = newHashMap();
		byteMap.forEach((k, v) -> ret.put(deserializa(k), deserializa(v)));
		return ret;
	}
	
	protected <T> Set<T> deserializa(Set<byte[]> bytes) {
		Set<T> ret = newHashSet();
		bytes.forEach(b -> ret.add(deserializa(b)));
		return ret;
	}

}
