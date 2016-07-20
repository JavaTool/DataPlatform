package dataplatform.coder.bytes;

import java.io.Serializable;

import dataplatform.util.SerializaUtil;

class SerialableCoder implements IStreamCoder {

	@Override
	public byte[] write(Object value) throws Exception {
		return SerializaUtil.serializable((Serializable) value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(byte[] stream) throws Exception {
		return (T) SerializaUtil.deserializable(stream);
	}

}
