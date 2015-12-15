package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

public interface IDataVisitor {
	
	<T> T get(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions);
	
	<T> List<T> getList(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions);
	
	<T> void save(T entity, VisitorType visitorType, Map<String, Object> conditions);
	
	<T> void delete(T entity, VisitorType visitorType, Map<String, Object> conditions);
	
	<T> void save(T[] entity, VisitorType visitorType, Map<String, Object> conditions);
	
	<T> void delete(T[] entity, VisitorType visitorType, Map<String, Object> conditions);

}
