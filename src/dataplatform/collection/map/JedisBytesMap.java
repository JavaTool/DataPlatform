package dataplatform.collection.map;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import dataplatform.cache.redis.IJedisSource;
import dataplatform.coder.bytes.IBytesCoder;
import redis.clients.jedis.Jedis;

class JedisBytesMap<K, V> implements IExtendMap<K, V> {
	
	private static final Logger log = LoggerFactory.getLogger(JedisBytesMap.class);
	
	private final IJedisSource source;
	
	private final byte[] key;
	
	private final IBytesCoder keyCoder;
	
	private final IBytesCoder valueCoder;
	
	public JedisBytesMap(IJedisSource source, String key, IBytesCoder keyCoder, IBytesCoder valueCoder) {
		this.source = source;
		byte[] bytes;
		try {
			bytes = keyCoder.write(key);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			bytes = null;
		}
		this.key = bytes;
		this.keyCoder = keyCoder;
		this.valueCoder = valueCoder;
	}
	
	private byte[] writeKey(Object key) throws Exception {
		return keyCoder.write(key);
	}
	
	private byte[] writeValue(Object value) throws Exception {
		return valueCoder.write(value);
	}
	
	@SuppressWarnings("unchecked")
	private V readValue(byte[] bytes) throws Exception {
		return (V) valueCoder.read(bytes);
	}
	
	@SuppressWarnings("unchecked")
	private K readKey(byte[] bytes) throws Exception {
		return (K) keyCoder.read(bytes);
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
			return jedis.hexists(this.key, writeKey(key));
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
			return jedis.hgetAll(key).containsValue(writeValue(value));
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public V get(Object key) {
		Jedis jedis = source.getJedis();
		try {
			return readValue(jedis.hget(this.key, writeKey(key)));
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public V put(K key, V value) {
		Jedis jedis = source.getJedis();
		try {
			jedis.hset(this.key, writeKey(key), writeValue(value));
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		} finally {
			source.useFinish(jedis);
		}
		return null;
	}

	@Override
	public V remove(Object key) {
		Jedis jedis = source.getJedis();
		try {
			V value = get(key);
			jedis.hdel(this.key, writeKey(key));
			return value;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		Jedis jedis = source.getJedis();
		try {
			Map<byte[], byte[]> map = Maps.newHashMap();
			m.forEach((k, v) -> {
				try {
					map.put(writeKey(k), writeValue(v));
				} catch (Exception e) {
					log.error("Error on RedisMap key : " + key, e);
				}
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
	public Set<K> keySet() {
		Jedis jedis = source.getJedis();
		try {
			Set<K> set = Sets.newHashSet();
			jedis.hkeys(key).forEach((bytes) -> {
				try {
					set.add(readKey(bytes));
				} catch (Exception e) {
					log.error("Error on RedisMap key : " + key, e);
				}
			});
			return set;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public Collection<V> values() {
		Jedis jedis = source.getJedis();
		try {
			Collection<byte[]> bytes = jedis.hvals(key);
			Collection<V> collection = Lists.newArrayListWithCapacity(bytes.size());
			bytes.forEach((v) -> {
				try {
					collection.add(readValue(v));
				} catch (Exception e) {
					log.error("Error on RedisMap key : " + key, e);
				}
			});
			return collection;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		Jedis jedis = source.getJedis();
		try {
			Set<java.util.Map.Entry<K, V>> set = Sets.newHashSet();
			jedis.hgetAll(key).forEach((k, v) -> {
				try {
					set.add(new JedisMapEntry(readKey(k), readValue(v)));
				} catch (Exception e) {
					log.error("Error on RedisMap key : " + key, e);
				}
			});
			return set;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}
	
	private class JedisMapEntry implements java.util.Map.Entry<K, V> {
		
		private final K key;
		
		private V value;
		
		public JedisMapEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}
	
		@Override
		public V getValue() {
			return value;
		}
	
		@Override
		public V setValue(V value) {
			V old = this.value;
			this.value = value;
			put(key, value);
			return old;
		}
		
	}

	@Override
	public List<V> mget(Object... keys) {
		byte[][] bytes = new byte[keys.length][];
		try {
			for (int i = 0;i < keys.length;i++) {
				bytes[i] = keyCoder.write(keys[i]);
			}
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		}
		
		Jedis jedis = source.getJedis();
		try {
			List<V> ret = Lists.newArrayListWithCapacity(keys.length);
			jedis.mget(bytes).forEach((data) -> {
				try {
					ret.add(readValue(data));
				} catch (Exception e) {
					log.error("Error on RedisMap key : " + key, e);
				}
			});
			return ret;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

}
