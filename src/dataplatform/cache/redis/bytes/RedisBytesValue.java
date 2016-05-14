package dataplatform.cache.redis.bytes;

import static dataplatform.util.DateUtil.toMilliTime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import dataplatform.cache.ICacheValue;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;
import redis.clients.jedis.Jedis;

public class RedisBytesValue extends ExistsJedisReources implements ICacheValue<byte[], byte[]> {
	
	public RedisBytesValue(IJedisReources jedisReources) {
		setResouces(jedisReources);
	}

	@Override
	public void append(byte[] key, byte[] value) {
		Jedis jedis = getJedis();
		try {
			jedis.append(key, value);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long decr(byte[] key, long decrement) {
		Jedis jedis = getJedis();
		try {
			return jedis.decrBy(key, decrement);
		} catch (Exception e) {
			error("", e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public byte[] get(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.get(key);
		} catch (Exception e) {
			error("", e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public List<byte[]> get(Object... keys) {
		Jedis jedis = getJedis();
		try {
			return jedis.mget((byte[][]) keys);
		} catch (Exception e) {
			error("", e);
			return Lists.newLinkedList();
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long incr(byte[] key, long increment) {
		Jedis jedis = getJedis();
		try {
			return jedis.incrBy(key, increment);
		} catch (Exception e) {
			error("", e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void set(byte[] key, byte[] value) {
		Jedis jedis = getJedis();
		try {
			jedis.set(key, value);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void set(byte[] key, byte[] value, boolean exists, long time, TimeUnit timeUnit) {
		time = toMilliTime(time, timeUnit);
		
		Jedis jedis = getJedis();
		try {
			jedis.set(key, value, (exists ? "XX".getBytes() : "NX".getBytes()), "PX".getBytes(), time);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void set(Map<byte[], byte[]> map) {
		byte[][] keyValues = new byte[map.size() << 1][];
		int index = 0;
		for (byte[] key : map.keySet()) {
			keyValues[index << 1] = key;
			keyValues[(index << 1) + 1] = map.get(key);
			++index;
		}
		
		Jedis jedis = getJedis();
		try {
			jedis.mset(keyValues);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

}
