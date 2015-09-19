package dataplatform.cache;

import java.io.Serializable;

public interface IStreamCoder {
	
	byte[] write(Serializable value) throws Exception;
	
	Serializable read(byte[] stream) throws Exception;

}
