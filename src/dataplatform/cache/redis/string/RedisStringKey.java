package dataplatform.cache.redis.string;

import static dataplatform.cache.ICacheKey.ValueType.None;
import static dataplatform.cache.ICacheKey.ValueType.valueof;
import static dataplatform.util.DateUtil.toMilliTime;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import dataplatform.cache.ICacheKey;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;

public class RedisStringKey extends ExistsJedisReources implements ICacheKey<String> {
	
	private static final Set<String> EMPTY_SET = ImmutableSet.of();
	
	public RedisStringKey(IJedisReources resources) {
		setResouces(resources);
	}

	@Override
	public void delete(Object... keys) {
		exec((jedis) -> jedis.del((String[]) keys));
	}

	@Override
	public boolean exists(String key) {
		return exec((jedis) -> {
			return jedis.exists(key);
		}, false);
	}

	@Override
	public void expire(String key, long time, TimeUnit timeUnit) {
		exec((jedis) -> jedis.pexpire(key, toMilliTime(time, timeUnit)));
	}

	@Override
	public void expireat(String key, long timestamp) {
		exec((jedis) -> jedis.pexpireAt(key, timestamp));
	}

	@Override
	public Set<String> keys(String pattern) {
		return exec((jedis) -> {
			return jedis.keys(pattern);
		}, EMPTY_SET);
	}

	@Override
	public void persist(String key) {
		exec((jedis) -> jedis.persist(key));
	}

	@Override
	public long ttl(String key) {
		return exec((jedis) -> {
			return jedis.pttl(key);
		}, 0L);
	}

	@Override
	public void rename(String key, String newkey) {
		exec((jedis) -> jedis.rename(key, newkey));
	}

	@Override
	public ValueType type(String key) {
		return exec((jedis) -> {
			return valueof(jedis.type(key));
		}, None);
	}

}
