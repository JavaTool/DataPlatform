package dataplatform.pubsub.impl;

public interface IByteArrayMessage {
	
	String getIp();
	
	String getMessageId();
	
	byte[] getBytes();

}
