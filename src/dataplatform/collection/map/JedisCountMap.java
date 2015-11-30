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
import redis.clients.jedis.Jedis;

class JedisCountMap implements ICountMap {
	
	private static final Logger log = LoggerFactory.getLogger(JedisCountMap.class);
	
	private final IJedisSource source;
	
	private final String key;
	
	private final IExtendMap<String, String> stringMap;

	public JedisCountMap(IJedisSource source, String key) {
		this.source = source;
		this.key = key;
		stringMap = ExtendMaps.newJedisStringMap(source, key);
	}

	@Override
	public int incrBy(String key, int value) {
		Jedis jedis = source.getJedis();
		try {
			return jedis.hincrBy(this.key, key, value).intValue();
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return 0;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public List<Integer> mget(Object... keys) {
		return null;
	}

	@Override
	public int size() {
		return stringMap.size();
	}

	@Override
	public boolean isEmpty() {
		return stringMap.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return stringMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return stringMap.containsValue(value);
	}

	@Override
	public Integer get(Object key) {
		return Integer.parseInt(stringMap.get(key));
	}

	@Override
	public Integer put(String key, Integer value) {
		stringMap.put(key, value.toString());
		return null;
	}

	@Override
	public Integer remove(Object key) {
		String ret = stringMap.remove(key);
		return ret == null || ret.length() == 0 ? null : Integer.parseInt(ret);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Integer> m) {
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
		stringMap.clear();
	}

	@Override
	public Set<String> keySet() {
		return stringMap.keySet();
	}

	@Override
	public Collection<Integer> values() {
		List<Integer> values = Lists.newLinkedList();
		stringMap.values().forEach(value -> values.add(Integer.parseInt(value)));
		return values;
	}

	@Override
	public Set<java.util.Map.Entry<String, Integer>> entrySet() {
		Set<java.util.Map.Entry<String, Integer>> set = Sets.newHashSet();
		stringMap.entrySet().forEach(entry -> new IntegerEntry(entry));
		return set;
	}
	
	private class IntegerEntry implements java.util.Map.Entry<String, Integer> {
		
		private final String key;
		
		private Integer value; 
		
		public IntegerEntry(java.util.Map.Entry<String, String> entry) {
			key = entry.getKey();
			value = Integer.parseInt(entry.getValue());
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public Integer getValue() {
			return value;
		}

		@Override
		public Integer setValue(Integer value) {
			Integer old = this.value;
			this.value = value;
			put(key, value);
			return old;
		}
		
	}

}
