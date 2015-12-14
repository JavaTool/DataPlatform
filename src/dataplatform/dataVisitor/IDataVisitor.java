package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

public interface IDataVisitor {
	
	<T> T get(Class<T> clz, EntityType entityType, Map<String, Object> conditions);
	
	<T> List<T> getList(Class<T> clz, EntityType entityType, Map<String, Object> conditions);
	
	<T> void save(T entity, EntityType entityType, Map<String, Object> conditions);
	
	<T> void delete(T entity, EntityType entityType, Map<String, Object> conditions);
	
	<T> void save(T[] entity, EntityType entityType, Map<String, Object> conditions);
	
	<T> void delete(T[] entity, EntityType entityType, Map<String, Object> conditions);

}
