package dataplatform.cache.redis.bytes;

import static dataplatform.util.DateUtil.toMilliTime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import dataplatform.cache.ICacheValue;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;

public class RedisBytesValue extends ExistsJedisReources implements ICacheValue<byte[], byte[]> {
	
	private static final List<byte[]> EMPTY_LIST = ImmutableList.of();
	
	public RedisBytesValue(IJedisReources jedisReources) {
		setResouces(jedisReources);
	}

	@Override
	public void append(byte[] key, byte[] value) {
		exec((jedis) -> jedis.append(key, value));
	}

	@Override
	public long decr(byte[] key, long decrement) {
		return exec((jedis) -> {
			return jedis.decrBy(key, decrement);
		}, 0L);
	}

	@Override
	public byte[] get(byte[] key) {
		return exec((jedis) -> {
			return jedis.get(key);
		}, null);
	}

	@Override
	public List<byte[]> multiGet(Object... keys) {
		return exec((jedis) -> {
			return jedis.mget((byte[][]) keys);
		}, EMPTY_LIST);
	}

	@Override
	public long incr(byte[] key, long increment) {
		return exec((jedis) -> {
			return jedis.incrBy(key, increment);
		}, 0L);
	}

	@Override
	public void set(byte[] key, byte[] value) {
		exec((jedis) -> jedis.set(key, value));
	}

	@Override
	public boolean xSet(byte[] key, byte[] value, boolean exists, long time, TimeUnit timeUnit) {
		return exec(jedis -> {
			return jedis.set(key, value, (exists ? "XX".getBytes() : "NX".getBytes()), "PX".getBytes(), toMilliTime(time, timeUnit)).equals("OK");
		}, false);
	}

	@Override
	public void multiSet(Map<byte[], byte[]> map) {
		byte[][] keyValues = new byte[map.size() << 1][];
		int index = 0;
		for (byte[] key : map.keySet()) {
			keyValues[index << 1] = key;
			keyValues[(index << 1) + 1] = map.get(key);
			++index;
		}

		exec((jedis) -> jedis.mset(keyValues));
	}

}
