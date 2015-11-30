package dataplatform.coder.bytes;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.MutableClassToInstanceMap;

public final class ByteCoders {
	
	private static final ClassToInstanceMap<IBytesCoder> map = MutableClassToInstanceMap.create();
	
	private ByteCoders() {}
	
	public static IBytesCoder newByteArrayCoder() {
		return map.getOrDefault(ByteArrayCoder.class, new ByteArrayCoder());
	}
	
	public static IBytesCoder newSerialableCoder() {
		return map.getOrDefault(SerialableCoder.class, new SerialableCoder());
	}

}
