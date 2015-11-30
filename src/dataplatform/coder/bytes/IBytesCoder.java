package dataplatform.coder.bytes;

public interface IBytesCoder {
	
	byte[] write(Object value) throws Exception;
	
	Object read(byte[] stream) throws Exception;

}
