package dataplatform.coder.string;

class FloatStringCoder extends SimpleStringCoder<Float> {

	@Override
	public Float parse(String str) {
		return Float.parseFloat(str);
	}

}
