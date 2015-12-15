package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public final class MultiDataVisitor implements IMultiDataVisitor {
	
	@SuppressWarnings("rawtypes")
	private final Map<Class, IDataVisitor> typeVisitors;
	
	private final IDataVisitor dataVistor;
	
	public MultiDataVisitor(IDataVisitor dataVistor) {
		this.dataVistor = dataVistor;
		typeVisitors = Maps.newHashMap();
	}

	@Override
	public <T> T get(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		return getDataVisitor(clz).get(clz, visitorType, conditions);
	}
	
	private <T> IDataVisitor getDataVisitor(Class<T> clz) {
		return typeVisitors.containsKey(clz) ? typeVisitors.get(clz) : dataVistor;
	}

	@Override
	public <T> List<T> getList(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		return getDataVisitor(clz).getList(clz, visitorType, conditions);
	}

	@Override
	public <T> void save(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		getDataVisitor(entity.getClass()).save(entity, visitorType, conditions);
	}

	@Override
	public <T> void delete(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		getDataVisitor(entity.getClass()).delete(entity, visitorType, conditions);
	}

	@Override
	public <T> void save(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		getDataVisitor(entity[0].getClass()).save(entity, visitorType, conditions);
	}

	@Override
	public <T> void delete(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		getDataVisitor(entity[0].getClass()).delete(entity, visitorType, conditions);
	}

	@Override
	public <T> void addDataVisitor(Class<T> clz, IDataVisitor dataVisitor) {
		typeVisitors.put(clz, dataVisitor);
	}

}
