package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import dataplatform.cache.ICacheUnit;

interface ICacheExecutor {
	
	Object exec(Serializable key, Serializable name, Object object, ICacheUnit cacheUnit);
	
	Object exec(Serializable key, Map<Serializable, Object> map, ICacheUnit cacheUnit, Collection<Object> list, Serializable... names);

}
