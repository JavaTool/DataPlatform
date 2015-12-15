package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

public abstract class FileVisitor implements IDataVisitor {

	@Override
	public <T> T get(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<T> getList(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> void save(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void delete(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void save(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void delete(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		// TODO Auto-generated method stub

	}

}
