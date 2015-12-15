package dataplatform.dataVisitor;

public final class DataVisitors {
	
	private DataVisitors() {}
	
	public static IDataVisitor createMapVisitor() {
		return new MapVisitor();
	}

}
