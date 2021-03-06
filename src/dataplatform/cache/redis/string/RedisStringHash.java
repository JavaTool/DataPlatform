package dataplatform.cache.redis.string;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import dataplatform.cache.ICacheHash;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;

public class RedisStringHash extends ExistsJedisReources implements ICacheHash<String, String, String> {
	
	private static final Set<String> EMPTY_SET = ImmutableSet.of();
	
	private static final List<String> EMPTY_LIST = ImmutableList.of();
	
	private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
	
	public RedisStringHash(IJedisReources resources) {
		setResouces(resources);
	}

	@Override
	public void remove(String key, Object... fields) {
		if (fields != null && fields.length > 0) {
			exec((jedis) -> jedis.hdel(key, (String[]) fields));
		}
	}

	@Override
	public boolean contains(String key, String field) {
		return exec((jedis) -> {
			return jedis.hexists(key, field);
		}, false);
	}

	@Override
	public String get(String key, String field) {
		return exec((jedis) -> {
			return jedis.hget(key, field);
		}, null);
	}

	@Override
	public List<String> multiGet(String key, Object... fields) {
		return exec((jedis) -> {
			return jedis.hmget(key, (String[]) fields);
		}, EMPTY_LIST);
	}

	@Override
	public Map<String, String> getAll(String key) {
		return exec((jedis) -> {
			return jedis.hgetAll(key);
		}, EMPTY_MAP);
	}

	@Override
	public Set<String> fields(String key) {
		return exec((jedis) -> {
			return jedis.hkeys(key);
		}, EMPTY_SET);
	}

	@Override
	public long size(String key) {
		return exec((jedis) -> {
			return jedis.hlen(key);
		}, 0L);
	}

	@Override
	public void multiSet(String key, Map<String, String> map) {
		if (map != null && map.size() > 0) {
			exec((jedis) -> jedis.hmset(key, map));
		}
	}

	@Override
	public void set(String key, String field, String value) {
		exec((jedis) -> jedis.hset(key, field, value));
	}

	@Override
	public List<String> values(String key) {
		return exec((jedis) -> {
			return jedis.hvals(key);
		}, EMPTY_LIST);
	}

}
