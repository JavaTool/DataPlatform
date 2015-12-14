package dataplatform.dataVisitor;

public interface IMultiDataVisitor extends IDataVisitor {
	
	<T> void addDataVisitor(Class<T> clz, IDataVisitor dataVisitor);

}
