package dataplatform.cache.redis.bytes;

import static dataplatform.util.DateUtil.toMilliTime;
import static dataplatform.cache.ICacheKey.ValueType.valueof;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import dataplatform.cache.ICacheKey;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;
import redis.clients.jedis.Jedis;

public class RedisBytesKey extends ExistsJedisReources implements ICacheKey<byte[]> {
	
	public RedisBytesKey(IJedisReources jedisReources) {
		setResouces(jedisReources);
	}

	@Override
	public void delete(Object... keys) {
		Jedis jedis = getJedis();
		try {
			jedis.del((byte[][]) keys);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public boolean exists(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.exists(key);
		} catch (Exception e) {
			error("", e);
			return false;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void expire(byte[] key, long time, TimeUnit timeUnit) {
		time = toMilliTime(time, timeUnit);
		
		Jedis jedis = getJedis();
		try {
			jedis.pexpire(key, time);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void expireat(byte[] key, long timestamp, TimeUnit timeUnit) {
		Jedis jedis = getJedis();
		try {
			jedis.pexpireAt(key, timestamp);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Set<byte[]> keys(byte[] pattern) {
		Jedis jedis = getJedis();
		try {
			return jedis.keys(pattern);
		} catch (Exception e) {
			error("", e);
			return Sets.newHashSet();
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void persist(byte[] key) {
		Jedis jedis = getJedis();
		try {
			jedis.persist(key);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long ttl(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.pttl(key);
		} catch (Exception e) {
			error("", e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void rename(byte[] key, byte[] newkey) {
		Jedis jedis = getJedis();
		try {
			jedis.rename(key, newkey);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public ValueType type(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return valueof(jedis.type(key));
		} catch (Exception e) {
			error("", e);
			return ValueType.None;
		} finally {
			useFinish(jedis);
		}
	}

}
