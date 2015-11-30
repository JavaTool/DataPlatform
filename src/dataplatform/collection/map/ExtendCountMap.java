package dataplatform.collection.map;

import java.util.Map;

class ExtendCountMap extends ExtendMap<String, Integer> implements ICountMap {

	public ExtendCountMap(Map<String, Integer> map) {
		super(map);
	}

	@Override
	public int incrBy(String key, int value) {
		int ret;
		if (containsKey(key)) {
			ret = value;
		} else {
			ret = get(key);
			ret += value;
		}
		put(key, ret);
		return ret;
	}

}
