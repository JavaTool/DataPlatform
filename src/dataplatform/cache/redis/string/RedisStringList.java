package dataplatform.cache.redis.string;

import dataplatform.cache.ICacheList;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;

public class RedisStringList extends ExistsJedisReources implements ICacheList<String, String> {
	
	public RedisStringList(IJedisReources resources) {
		setResouces(resources);
	}

	@Override
	public String headPop(String key) {
		return exec((jedis) -> {
			return jedis.lpop(key);
		}, null);
	}

	@Override
	public void tailPush(String key, Object... objects) {
		exec((jedis) -> jedis.lpush(key, (String[]) objects));
	}

	@Override
	public String get(String key, long index) {
		return exec((jedis) -> {
			return jedis.lindex(key, index);
		}, null);
	}

	@Override
	public long size(String key) {
		return exec((jedis) -> {
			return jedis.llen(key);
		}, 0L);
	}

	@Override
	public void trim(String key, long start, long end) {
		exec((jedis) -> jedis.ltrim(key, start, end));
	}

}
