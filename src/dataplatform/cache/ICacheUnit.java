package dataplatform.cache;

import dataplatform.coder.bytes.IBytesCoder;

public interface ICacheUnit {
	
	String getKey();
	
	@SuppressWarnings("rawtypes")
	Class getValueClass();
	
	IBytesCoder getStreamCoder();
	
	boolean delAtShutdown();

}
