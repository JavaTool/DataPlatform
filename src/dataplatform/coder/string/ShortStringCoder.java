package dataplatform.coder.string;

class ShortStringCoder extends SimpleStringCoder<Short> {

	@Override
	public Short parse(String str) {
		return Short.parseShort(str);
	}

}
