package dataplatform.cache.redis;

import dataplatform.cache.ICacheUnit;
import dataplatform.coder.bytes.ByteCoders;
import dataplatform.coder.bytes.IBytesCoder;

class CacheUnitFactory {
	
	static final IBytesCoder defaultStreamCoder = ByteCoders.newSerialableCoder();
	
	@SuppressWarnings("rawtypes")
	private static final Class[] STRING_CLASSES = new Class[]{
		Byte.class, Short.class, Integer.class, Long.class, Boolean.class, String.class, Double.class, Float.class, 
	};
	
	private CacheUnitFactory() {}
	
	public static ICacheUnit createCacheUnit(String key, @SuppressWarnings("rawtypes") Class valueClass, boolean delAtShutdown, IBytesCoder streamCoder) {
		streamCoder = streamCoder == null ? createStreamCoder(valueClass) : streamCoder;
		return new CacheUnit(key, valueClass, delAtShutdown, streamCoder);
	}
	
	@SuppressWarnings("rawtypes")
	private static IBytesCoder createStreamCoder(Class valueClass) {
		for (Class cls : STRING_CLASSES) {
			if (cls.equals(valueClass)) {
				return null;
			}
		}
		return defaultStreamCoder;
	}
	
	private static class CacheUnit implements ICacheUnit {
		
		private String key;
		
		@SuppressWarnings("rawtypes")
		private Class valueClass;
		
		private boolean delAtShutdown;
		
		private IBytesCoder streamCoder;
		
		public CacheUnit(String key, @SuppressWarnings("rawtypes") Class valueClass, boolean delAtShutdown, IBytesCoder streamCoder) {
			this.key = key;
			this.valueClass = valueClass;
			this.delAtShutdown = delAtShutdown;
			this.streamCoder = streamCoder;
		}

		@Override
		public String getKey() {
			return key;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Class getValueClass() {
			return valueClass;
		}

		@Override
		public IBytesCoder getStreamCoder() {
			return streamCoder;
		}

		@Override
		public boolean delAtShutdown() {
			return delAtShutdown;
		}
		
	}

}
