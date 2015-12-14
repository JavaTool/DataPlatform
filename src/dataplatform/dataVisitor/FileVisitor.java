package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

public abstract class FileVisitor implements IDataVisitor {

	@Override
	public <T> T get(Class<T> clz, EntityType entityType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<T> getList(Class<T> clz, EntityType entityType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> void save(T entity, EntityType entityType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void delete(T entity, EntityType entityType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void save(T[] entity, EntityType entityType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void delete(T[] entity, EntityType entityType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

}
