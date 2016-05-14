package dataplatform.coder.bytes;

import java.util.Map;

import com.google.common.collect.Maps;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

class ProtoStuffCoder implements IBytesCoder {
	
	@SuppressWarnings("rawtypes")
	private final Map<Class, Schema> schemas = Maps.newHashMap();

	@SuppressWarnings("unchecked")
	@Override
	public  byte[] write(Object value) throws Exception {
		// 获取概要
		@SuppressWarnings("rawtypes")
		Schema schema = getSchema(value.getClass());
		// 序列化
		LinkedBuffer buffer = LinkedBuffer.allocate(4096);
	    try {
	        return ProtostuffIOUtil.toByteArray(value, schema, buffer);
	    } finally {
	        buffer.clear();
	    }
	}

	@Override
	public <T> T read(byte[] stream, Class<T> clz) throws Exception {
		if (stream == null) {
			return null;
		} else {
			Schema<T> schema = getSchema(clz);
			T account = clz.newInstance();
			ProtostuffIOUtil.mergeFrom(stream, account, schema);
			return account;
		}
	}
	
	private <T> Schema<T> getSchema(Class<T> clz) {
		@SuppressWarnings("unchecked")
		Schema<T> schema = schemas.get(clz);
		if (schema == null) {
			schema = RuntimeSchema.getSchema(clz);
			schemas.put(clz, schema);
		}
		return schema;
	}

	@Deprecated
	@Override
	public Object read(byte[] stream) throws Exception {
		return null;
	}

}
