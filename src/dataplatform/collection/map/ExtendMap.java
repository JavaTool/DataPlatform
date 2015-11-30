package dataplatform.collection.map;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

class ExtendMap<K, V> implements IExtendMap<K, V> {
	
	private final Map<K, V> map;
	
	public ExtendMap(Map<K, V> map) {
		this.map = map;
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		return map.put(key, value);
	}

	@Override
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	@Override
	public List<V> mget(Object... keys) {
		List<V> ret = Lists.newArrayListWithCapacity(keys.length);
		for (Object key : keys) {
			ret.add(map.get(key));
		}
		return ret;
	}

}
