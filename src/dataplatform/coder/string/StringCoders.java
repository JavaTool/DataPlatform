package dataplatform.coder.string;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.MutableClassToInstanceMap;

public final class StringCoders {
	
	@SuppressWarnings("rawtypes")
	private static final ClassToInstanceMap<IStringCoder> map = MutableClassToInstanceMap.create();
	
	private StringCoders() {}
	
	@SuppressWarnings("unchecked")
	public static IStringCoder<Byte> newByteStringCoder() {
		return map.getOrDefault(ByteStringCoder.class, new ByteStringCoder());
	}
	
	@SuppressWarnings("unchecked")
	public static IStringCoder<Short> newShortStringCoder() {
		return map.getOrDefault(ShortStringCoder.class, new ShortStringCoder());
	}
	
	@SuppressWarnings("unchecked")
	public static IStringCoder<Integer> newIntegerStringCoder() {
		return map.getOrDefault(IntegerStringCoder.class, new IntegerStringCoder());
	}
	
	@SuppressWarnings("unchecked")
	public static IStringCoder<Long> newLongStringCoder() {
		return map.getOrDefault(LongStringCoder.class, new LongStringCoder());
	}
	
	@SuppressWarnings("unchecked")
	public static IStringCoder<Double> newDoubleStringCoder() {
		return map.getOrDefault(DoubleStringCoder.class, new DoubleStringCoder());
	}
	
	@SuppressWarnings("unchecked")
	public static IStringCoder<Float> newFloatStringCoder() {
		return map.getOrDefault(FloatStringCoder.class, new FloatStringCoder());
	}

}
