package dataplatform.cache.redis.bytes;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import dataplatform.cache.ICacheHash;
import dataplatform.cache.redis.ExistsJedisReources;
import dataplatform.cache.redis.IJedisReources;
import redis.clients.jedis.Jedis;

public class RedisBytesHash extends ExistsJedisReources implements ICacheHash<byte[], byte[], byte[]> {
	
	public RedisBytesHash(IJedisReources jedisReources) {
		setResouces(jedisReources);
	}

	@Override
	public void remove(byte[] key, Object... fields) {
		Jedis jedis = getJedis();
		try {
			jedis.hdel(key, (byte[][]) fields);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public boolean contains(byte[] key, byte[] field) {
		Jedis jedis = getJedis();
		try {
			return jedis.hexists(key, field);
		} catch (Exception e) {
			error("", e);
			return false;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public byte[] get(byte[] key, byte[] field) {
		Jedis jedis = getJedis();
		try {
			return jedis.hget(key, field);
		} catch (Exception e) {
			error("", e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public List<byte[]> get(byte[] key, Object... fields) {
		Jedis jedis = getJedis();
		try {
			return jedis.hmget(key, (byte[][]) fields);
		} catch (Exception e) {
			error("", e);
			return Lists.newArrayList();
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Map<byte[], byte[]> getAll(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.hgetAll(key);
		} catch (Exception e) {
			error("", e);
			return Maps.newHashMap();
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public List<byte[]> fields(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return Lists.newArrayList(jedis.hkeys(key));
		} catch (Exception e) {
			error("", e);
			return Lists.newArrayList();
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long size(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.hlen(key);
		} catch (Exception e) {
			error("", e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void set(byte[] key, Map<byte[], byte[]> map) {
		Jedis jedis = getJedis();
		try {
			jedis.hmset(key, map);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void set(byte[] key, byte[] field, byte[] value) {
		Jedis jedis = getJedis();
		try {
			jedis.hset(key, field, value);
		} catch (Exception e) {
			error("", e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public List<byte[]> values(byte[] key) {
		Jedis jedis = getJedis();
		try {
			return jedis.hvals(key);
		} catch (Exception e) {
			error("", e);
			return Lists.newArrayList();
		} finally {
			useFinish(jedis);
		}
	}

}
