package dataplatform.coder.string;

class ByteStringCoder extends SimpleStringCoder<Byte> {

	@Override
	public Byte parse(String str) {
		return Byte.parseByte(str);
	}

}
