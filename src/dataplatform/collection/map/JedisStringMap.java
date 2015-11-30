package dataplatform.collection.map;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import dataplatform.cache.redis.IJedisSource;
import redis.clients.jedis.Jedis;

class JedisStringMap implements IExtendMap<String, String> {
	
	protected static final Logger log = LoggerFactory.getLogger(JedisStringMap.class);
	
	protected final IJedisSource source;
	
	protected final String key;
	
	public JedisStringMap(IJedisSource source, String key) {
		this.source = source;
		this.key = key;
	}

	@Override
	public int size() {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hlen(key).intValue();
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return 0;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hexists(this.key, key.toString());
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean containsValue(Object value) {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hgetAll(key).containsValue(value.toString());
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public String get(Object key) {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hget(this.key, key.toString());
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public String put(String key, String value) {
		Jedis jedis = source.getJedis();
		try {
			jedis.hset(this.key, key.toString(), value.toString());
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		} finally {
			source.useFinish(jedis);
		}
		return null;
	}

	@Override
	public String remove(Object key) {
		Jedis jedis = source.getJedis();
		try {
			String value = get(key);
			jedis.hdel(this.key, key.toString());
			return value;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		Jedis jedis = source.getJedis();
		try {
			Map<String, String> map = Maps.newHashMap();
			m.forEach((k, v) -> {
				map.put(k.toString(), v.toString());
			});
			jedis.hmset(key, map);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void clear() {
		Jedis jedis = source.getJedis();
		try {
			if ("hash".equals(jedis.type(key))) {
				jedis.del(key);
			}
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public Set<String> keySet() {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hkeys(key);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public Collection<String> values() {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hvals(key);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public Set<java.util.Map.Entry<String, String>> entrySet() {
		Jedis jedis = source.getJedis();
		try {
			Set<java.util.Map.Entry<String, String>> set = Sets.newHashSet();
			jedis.hgetAll(key).forEach((k, v) -> {
				set.add(new JedisMapEntry(k, v));
			});
			return set;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}
	
	private class JedisMapEntry implements java.util.Map.Entry<String, String> {
		
		private final String key;
		
		private String value;
		
		public JedisMapEntry(String key, String value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String getKey() {
			return key;
		}
	
		@Override
		public String getValue() {
			return value;
		}
	
		@Override
		public String setValue(String value) {
			String old = this.value;
			this.value = value;
			put(key, value);
			return old;
		}
		
	}

	@Override
	public List<String> mget(Object... keys) {
		Jedis jedis = source.getJedis();
		try {
			String[] fields = new String[keys.length];
			for (int i = 0;i < keys.length;i++) {
				fields[i] = keys[i].toString();
			}
			return jedis.hmget(key, fields);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

}
