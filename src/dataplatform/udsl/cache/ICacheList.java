package dataplatform.udsl.cache;

import java.util.List;

public interface ICacheList {
	
	Object lindex(String key, long index);
	
	long llen(String key);
	
	List<Object> lrange(String key, long start, long end);
	
	void ltrim(String key, long start, long end);

}
