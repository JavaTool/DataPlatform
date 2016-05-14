package dataplatform.coder.bytes;

public interface IBytesCoder {
	
	byte[] write(Object value) throws Exception;
	
	Object read(byte[] stream) throws Exception;
	
	<T> T read(byte[] stream, Class<T> clz) throws Exception;

}
