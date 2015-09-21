package dataplatform.cache.redis;

import java.util.Collection;
import java.util.Map;

import dataplatform.cache.ICacheUnit;

interface ICacheExecutor {
	
	Object exec(String key, String name, Object object, ICacheUnit cacheUnit);
	
	Object exec(String key, Map<String, Object> map, ICacheUnit cacheUnit, Collection<Object> list, String... names);

}
