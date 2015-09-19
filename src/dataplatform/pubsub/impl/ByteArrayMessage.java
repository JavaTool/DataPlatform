package dataplatform.pubsub.impl;

public class ByteArrayMessage implements IByteArrayMessage {
	
	private final byte[] datas;
	
	private final String messageId;
	
	private final String ip;
	
	public ByteArrayMessage(byte[] datas, String messageId, String ip) {
		this.ip = ip;
		this.datas = datas;
		this.messageId = messageId;
	}

	@Override
	public String getIp() {
		return ip;
	}

	@Override
	public String getMessageId() {
		return messageId;
	}

	@Override
	public byte[] getBytes() {
		return datas;
	}

}
