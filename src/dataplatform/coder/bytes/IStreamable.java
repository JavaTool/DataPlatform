package dataplatform.coder.bytes;

public interface IStreamable {
	
	byte[] toByteArray() throws Exception;
	
	void readFromByteArray(byte[] bytes) throws Exception;

}
