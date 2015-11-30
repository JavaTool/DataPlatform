package dataplatform.coder.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import dataplatform.pubsub.impl.ByteArrayMessage;
import dataplatform.pubsub.impl.IByteArrayMessage;

class ByteArrayCoder implements IBytesCoder {

	@Override
	public byte[] write(Object value) throws Exception {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bout);
		dos.writeUTF(((IByteArrayMessage) value).getIp());
		dos.writeUTF(((IByteArrayMessage) value).getMessageId());
		byte[] array = ((IByteArrayMessage) value).getBytes();
		dos.writeInt(array.length);
		dos.write(array);
		return bout.toByteArray();
	}

	@Override
	public IByteArrayMessage read(byte[] stream) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(stream);
		DataInputStream dis = new DataInputStream(bais);
		String ip = dis.readUTF();
		String messageId = dis.readUTF();
		byte[] datas = new byte[dis.readInt()];
		dis.read(datas);
		return new ByteArrayMessage(datas, messageId, ip);
	}

}
