package dataplatform.dataVisitor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

public class LayerDataVisitor implements ILayerDataVisitor {
	
	private final List<IDataVisitor> layerVisitors;
	
	public LayerDataVisitor() {
		layerVisitors = Lists.newLinkedList();
	}

	@Override
	public <T> T get(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		T value = null;
		switch (visitorType) {
		case LOAD : 
			for (IDataVisitor dataVisitor : layerVisitors) {
				if ((value = dataVisitor.get(clz, visitorType, conditions)) != null) {
					break;
				}
			}
			return value;
		case LOAD_CACHE : 
			for (int i = 0;i < layerVisitors.size();i++) {
				IDataVisitor dataVisitor = layerVisitors.get(i);
				if ((value = dataVisitor.get(clz, visitorType, conditions)) != null) {
					for (int j = 0;j < i;j++) {
						layerVisitors.get(j).save(value, visitorType, conditions);
					}
					break;
				}
			}
			return value;
		default : 
			return layerVisitors.get(0).get(clz, visitorType, conditions);
		}
	}

	@Override
	public <T> List<T> getList(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		List<T> list = null;
		switch (visitorType) {
		case LOAD : 
			for (IDataVisitor dataVisitor : layerVisitors) {
				if ((list = dataVisitor.getList(clz, visitorType, conditions)) != null) {
					return list;
				}
			}
			return Lists.newArrayList();
		case LOAD_CACHE : 
			for (int i = 0;i < layerVisitors.size();i++) {
				IDataVisitor dataVisitor = layerVisitors.get(i);
				if ((list = dataVisitor.getList(clz, visitorType, conditions)) != null) {
					for (int j = 0;j < i;j++) {
						layerVisitors.get(j).save(list.toArray(), visitorType, conditions);
					}
					return list;
				}
			}
			return Lists.newArrayList();
		default : 
			return layerVisitors.get(0).getList(clz, visitorType, conditions);
		}
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

	@Override
	public void addDataVisitor(int layer, IDataVisitor dataVisitor) {
		layerVisitors.add(layer, dataVisitor);
	}

}
