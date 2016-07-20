package dataplatform.cache.redis.bytes;

import dataplatform.cache.ICacheList;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;

public class RedisBytesList extends ExistsJedisReources implements ICacheList<byte[], byte[]> {
	
	public RedisBytesList(IJedisReources jedisReources) {
		setResouces(jedisReources);
	}

	@Override
	public byte[] headPop(byte[] key) {
		return exec((jedis) -> {
			return jedis.lpop(key);
		}, null);
	}

	@Override
	public void tailPush(byte[] key, Object... objects) {
		exec((jedis) -> jedis.lpush(key, (byte[][]) objects));
	}

	@Override
	public byte[] get(byte[] key, long index) {
		return exec((jedis) -> {
			return jedis.lindex(key, index);
		}, null);
	}

	@Override
	public long size(byte[] key) {
		return exec((jedis) -> {
			return jedis.llen(key);
		}, 0L);
	}

	@Override
	public void trim(byte[] key, long start, long end) {
		exec((jedis) -> jedis.ltrim(key, start, end));
	}

}
