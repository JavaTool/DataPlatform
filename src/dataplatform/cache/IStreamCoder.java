package dataplatform.cache;

public interface IStreamCoder {
	
	byte[] write(Object value) throws Exception;
	
	Object read(byte[] stream) throws Exception;

}
