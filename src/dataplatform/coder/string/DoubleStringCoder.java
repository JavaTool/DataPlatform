package dataplatform.coder.string;

class DoubleStringCoder extends SimpleStringCoder<Double> {

	@Override
	public Double parse(String str) {
		return Double.parseDouble(str);
	}

}
