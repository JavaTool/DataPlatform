package dataplatform.coder.bytes;

import java.io.Serializable;

import dataplatform.util.SerializaUtil;

class SerialableCoder implements IBytesCoder {

	@Override
	public byte[] write(Object value) throws Exception {
		return SerializaUtil.serializable((Serializable) value);
	}

	@Override
	public Object read(byte[] stream) throws Exception {
		return SerializaUtil.deserializable(stream);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(byte[] stream, Class<T> clz) throws Exception {
		return (T) read(stream);
	}

}
