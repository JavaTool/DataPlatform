package dataplatform.cache;

import java.io.Serializable;

public interface ICacheUnit {
	
	Serializable getKey();
	
	@SuppressWarnings("rawtypes")
	Class getValueClass();
	
	IStreamCoder getStreamCoder();
	
	boolean delAtShutdown();

}
