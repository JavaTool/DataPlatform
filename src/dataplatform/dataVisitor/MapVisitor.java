package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

final class MapVisitor implements IDataVisitor {
	
	private final Map<String, Object> map;
	
	public MapVisitor() {
		map = Maps.newConcurrentMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		return (T) map.get(getKey(conditions).toString());
	}
	
	private static Object getKey(Map<String, Object> conditions) {
		return conditions.get(CONDITION_KEY);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> getList(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		String[] keys = (String[]) getKey(conditions);
		List<T> list = Lists.newArrayListWithCapacity(keys.length);
		for (String key : keys) {
			list.add((T) map.get(key));
		}
		return list;
	}

	@Override
	public <T> void save(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		map.put(getKey(conditions).toString(), entity);
	}

	@Override
	public <T> void delete(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		map.remove(getKey(conditions).toString(), entity);
	}

	@Override
	public <T> void save(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		String[] keys = (String[]) getKey(conditions);
		for (int i = 0;i < keys.length;i++) {
			map.put(keys[i], entity[i]);
		}
	}

	@Override
	public <T> void delete(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		String[] keys = (String[]) getKey(conditions);
		for (int i = 0;i < keys.length;i++) {
			map.remove(keys[i], entity[i]);
		}
	}

}
