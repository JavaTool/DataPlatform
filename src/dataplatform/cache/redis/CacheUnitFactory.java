package dataplatform.cache.redis;

import java.io.Serializable;

import dataplatform.cache.ICacheUnit;
import dataplatform.cache.IStreamCoder;

class CacheUnitFactory {
	
	static final IStreamCoder defaultStreamCoder = new SerialableCoder();
	
	@SuppressWarnings("rawtypes")
	private static final Class[] STRING_CLASSES = new Class[]{
		Byte.class, Short.class, Integer.class, Long.class, Boolean.class, String.class, Double.class, Float.class, 
	};
	
	private CacheUnitFactory() {}
	
	public static ICacheUnit createCacheUnit(Serializable key, @SuppressWarnings("rawtypes") Class valueClass, boolean delAtShutdown, IStreamCoder streamCoder) {
		streamCoder = streamCoder == null ? createStreamCoder(valueClass) : streamCoder;
		return new CacheUnit(key, valueClass, delAtShutdown, streamCoder);
	}
	
	@SuppressWarnings("rawtypes")
	private static IStreamCoder createStreamCoder(Class valueClass) {
		for (Class cls : STRING_CLASSES) {
			if (cls.equals(valueClass)) {
				return null;
			}
		}
		return defaultStreamCoder;
	}
	
	private static class CacheUnit implements ICacheUnit {
		
		private Serializable key;
		
		@SuppressWarnings("rawtypes")
		private Class valueClass;
		
		private boolean delAtShutdown;
		
		private IStreamCoder streamCoder;
		
		public CacheUnit(Serializable key, @SuppressWarnings("rawtypes") Class valueClass, boolean delAtShutdown, IStreamCoder streamCoder) {
			this.key = key;
			this.valueClass = valueClass;
			this.delAtShutdown = delAtShutdown;
			this.streamCoder = streamCoder;
		}

		@Override
		public Serializable getKey() {
			return key;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Class getValueClass() {
			return valueClass;
		}

		@Override
		public IStreamCoder getStreamCoder() {
			return streamCoder;
		}

		@Override
		public boolean delAtShutdown() {
			return delAtShutdown;
		}
		
	}

}
