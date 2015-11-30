package dataplatform.util;

import dataplatform.coder.string.StringCoders;

public final class ArrayUtil {
	
	private ArrayUtil() {}
	
	@SuppressWarnings("unchecked")
	public static <T> T[] toNumberArray(String[] strs, T[] array) {
		if (array instanceof Byte[] || array.getClass().equals(byte[].class)) {
			for (int i = 0;i < array.length;i++) {
				array[i] = (T) StringCoders.newByteStringCoder().parse(strs[i]);
			}
		} else if (array instanceof Short[] || array.getClass().equals(short[].class)) {
			for (int i = 0;i < array.length;i++) {
				array[i] = (T) StringCoders.newShortStringCoder().parse(strs[i]);
			}
		} else if (array instanceof Integer[] || array.getClass().equals(int[].class)) {
			for (int i = 0;i < array.length;i++) {
				array[i] = (T) StringCoders.newIntegerStringCoder().parse(strs[i]);
			}
		} else if (array instanceof Long[] || array.getClass().equals(long[].class)) {
			for (int i = 0;i < array.length;i++) {
				array[i] = (T) StringCoders.newLongStringCoder().parse(strs[i]);
			}
		} else if (array instanceof Double[] || array.getClass().equals(double[].class)) {
			for (int i = 0;i < array.length;i++) {
				array[i] = (T) StringCoders.newDoubleStringCoder().parse(strs[i]);
			}
		} else if (array instanceof Float[] || array.getClass().equals(float[].class)) {
			for (int i = 0;i < array.length;i++) {
				array[i] = (T) StringCoders.newFloatStringCoder().parse(strs[i]);
			}
		}
		return array;
	}

}
