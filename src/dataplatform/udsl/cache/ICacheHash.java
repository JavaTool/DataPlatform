package dataplatform.udsl.cache;

import java.util.List;
import java.util.Map;

public interface ICacheHash {
	
	void hdel(String key, String... fields);
	
	boolean hexists(String key, String field);
	
	Object hget(String key, String field);
	
	Map<String, Object> hgetAll(String key);
	
	List<String> hkeys(String key);
	
	List<Object> hvalues(String key);

}
