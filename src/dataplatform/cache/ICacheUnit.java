package dataplatform.cache;

public interface ICacheUnit {
	
	String getKey();
	
	@SuppressWarnings("rawtypes")
	Class getValueClass();
	
	IStreamCoder getStreamCoder();
	
	boolean delAtShutdown();

}
