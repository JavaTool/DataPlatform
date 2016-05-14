package dataplatform.cache.object;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import dataplatform.coder.bytes.IBytesCoder;

abstract class StreamCoderCache {
	
	private final IBytesCoder bytesCoder;
	
	public StreamCoderCache(IBytesCoder streamCoder) {
		this.bytesCoder = streamCoder;
	}
	
	protected byte[] serializa(Object object) {
		try {
			return bytesCoder.write(object);
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
		Map<byte[], byte[]> byteMap = Maps.newHashMap();
		map.forEach((k, v) -> byteMap.put(serializa(k), serializa(v)));
		return byteMap;
	}
	
	protected <T> T deserializa(byte[] bytes, Class<T> clz) {
		try {
			return bytesCoder.read(bytes, clz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected <T> List<T> deserializa(List<byte[]> byteList, Class<T> clz) {
		List<T> ret = Lists.newArrayListWithCapacity(byteList.size());
		byteList.forEach(bytes -> ret.add(deserializa(bytes, clz)));
		return ret;
	}
	
	protected <K, V> Map<K, V> deserializa(Map<byte[], byte[]> byteMap, Class<K> kclz, Class<V> vclz) {
		Map<K, V> ret = Maps.newHashMap();
		byteMap.forEach((k, v) -> ret.put(deserializa(k, kclz), deserializa(v, vclz)));
		return ret;
	}
	
	protected <T> Set<T> deserializa(Set<byte[]> bytes, Class<T> clz) {
		Set<T> ret = Sets.newHashSet();
		bytes.forEach(b -> ret.add(deserializa(b, clz)));
		return ret;
	}

}
