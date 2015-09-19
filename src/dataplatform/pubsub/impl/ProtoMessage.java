package dataplatform.pubsub.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import com.google.protobuf.GeneratedMessage;

import dataplatform.pubsub.IPubsubMessage;

public class ProtoMessage implements IPubsubMessage {
	
	private final byte[] datas;
	
	public ProtoMessage(GeneratedMessage message, String messageId, String ip) throws Exception {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bout);
		dos.writeUTF(ip);
		dos.writeUTF(messageId);
		byte[] array = message.toByteArray();
		dos.writeInt(array.length);
		dos.write(array);
		datas = bout.toByteArray();
	}

	@Override
	public byte[] toByteArray() {
		return datas;
	}

}
