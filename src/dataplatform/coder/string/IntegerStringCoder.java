package dataplatform.coder.string;

class IntegerStringCoder extends SimpleStringCoder<Integer> {

	@Override
	public Integer parse(String str) {
		return Integer.parseInt(str);
	}

}
