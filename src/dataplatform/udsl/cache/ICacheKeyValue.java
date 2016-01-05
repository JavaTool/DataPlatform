package dataplatform.udsl.cache;

import java.util.Map;

public interface ICacheKeyValue {
	
	void set(String key, Object value);
	
	String get(String key);
	
	Map<String, Object> mGet(String key);
	
	void mSet(Map<String, Object> map);

}
