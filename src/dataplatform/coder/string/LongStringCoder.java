package dataplatform.coder.string;

class LongStringCoder extends SimpleStringCoder<Long> {

	@Override
	public Long parse(String str) {
		return Long.parseLong(str);
	}

}
