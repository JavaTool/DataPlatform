package dataplatform.collection.deque;

import java.util.Deque;

import dataplatform.cache.redis.IJedisSource;
import dataplatform.coder.bytes.ByteCoders;
import dataplatform.coder.bytes.IBytesCoder;

public final class ExtendDeques {
	
	private ExtendDeques() {}
	
	public static Deque<String> newStringDeque(IJedisSource source, String key) {
		return new JedisStringDeque(source, key);
	}
	
	public static <E> Deque<E> newJedisBytesMap(IJedisSource source, String key) {
		return newJedisBytesMap(source, key, ByteCoders.newSerialableCoder());
	}
	
	public static <E> Deque<E> newJedisBytesMap(IJedisSource source, String key, IBytesCoder valueCoder) {
		return new JedisBytesDeque<>(source, key, ByteCoders.newSerialableCoder(), valueCoder);
	}

}
