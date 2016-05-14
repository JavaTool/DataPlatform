package dataplatform.cache.redis.bytes;

import dataplatform.cache.ICacheList;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;
import redis.clients.jedis.Jedis;

public class RedisBytesList extends ExistsJedisReources implements ICacheList<byte[], byte[]> {
	
	public RedisBytesList(IJedisReources jedisReources) {
		setResouces(jedisReources);
	}

	@Override
	public byte[] headPop(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.lpop(key);
		} catch (Exception e) {
			error("", e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void tailPush(byte[] key, Object... objects) {
		Jedis jedis = getJedis();
		try {
			jedis.lpush(key, (byte[][]) objects);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public byte[] get(byte[] key, long index) {
		Jedis jedis = getJedis();
		try {
			return jedis.lindex(key, index);
		} catch (Exception e) {
			error("", e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long size(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.llen(key);
		} catch (Exception e) {
			error("", e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void trim(byte[] key, long start, long end) {
		Jedis jedis = getJedis();
		try {
			jedis.ltrim(key, start, end);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

}
