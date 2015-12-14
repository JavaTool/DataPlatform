package dataplatform.dataVisitor;

public interface ILayerDataVisitor extends IDataVisitor {
	
	void addDataVisitor(int layer, IDataVisitor dataVisitor);

}
