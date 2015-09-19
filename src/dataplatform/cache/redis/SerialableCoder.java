package dataplatform.cache.redis;

import java.io.Serializable;

import dataplatform.cache.IStreamCoder;
import dataplatform.util.SerializaUtil;

public class SerialableCoder implements IStreamCoder {

	@Override
	public byte[] write(Object value) throws Exception {
		return SerializaUtil.serializable((Serializable) value);
	}

	@Override
	public Object read(byte[] stream) throws Exception {
		return SerializaUtil.deserializable(stream);
	}

}
