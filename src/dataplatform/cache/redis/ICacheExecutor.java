package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import dataplatform.cache.ICacheUnit;

interface ICacheExecutor {
	
	Serializable exec(Serializable key, Serializable name, Serializable object, ICacheUnit cacheUnit);
	
	Serializable exec(Serializable key, Map<Serializable, Serializable> map, ICacheUnit cacheUnit, Collection<Serializable> list, Serializable... names);

}
