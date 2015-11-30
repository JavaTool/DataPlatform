package dataplatform.collection.map;

import java.util.Map;

import dataplatform.cache.redis.IJedisSource;
import dataplatform.coder.bytes.ByteCoders;
import dataplatform.coder.bytes.IBytesCoder;

public final class ExtendMaps {
	
	private ExtendMaps() {}
	
	public static IExtendMap<String, String> newJedisStringMap(IJedisSource source, String key) {
		return new JedisStringMap(source, key);
	}
	
	public static <K, V> Map<K, V> newJedisBytesMap(IJedisSource source, String key) {
		return newJedisBytesMap(source, key, ByteCoders.newSerialableCoder());
	}
	
	public static <K, V> Map<K, V> newJedisBytesMap(IJedisSource source, String key, IBytesCoder valueCoder) {
		return newJedisBytesMap(source, key, valueCoder, valueCoder);
	}
	
	public static <K, V> Map<K, V> newJedisBytesMap(IJedisSource source, String key, IBytesCoder keyCoder, IBytesCoder valueCoder) {
		return new JedisBytesMap<>(source, key, keyCoder, valueCoder);
	}
	
	public static <K, V> Map<K, V> newExtendMap(Map<K, V> map) {
		return new ExtendMap<K, V>(map);
	}
	
	public static ICountMap newCountMap(IJedisSource source, String key) {
		return new JedisCountMap(source, key);
	}
	
	public static ICountMap newCountMap(Map<String, Integer> map) {
		return new ExtendCountMap(map);
	}

}
