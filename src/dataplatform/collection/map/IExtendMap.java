package dataplatform.collection.map;

import java.util.List;
import java.util.Map;

public interface IExtendMap<K, V> extends Map<K, V> {
	
	List<V> mget(Object... keys);

}
